const std = @import("std");
const sql_runtime = @import("../sql/runtime.zig");

pub const Error = std.mem.Allocator.Error || std.fs.File.WriteError || error{EmitFailed};

pub const Sink = struct {
    context: *anyopaque,
    vtable: *const VTable,

    pub fn emit(self: Sink, row: *const sql_runtime.Row) Error!void {
        try self.vtable.emit(self.context, row);
    }

    pub fn emitBatch(self: Sink, rows: []const sql_runtime.Row) Error!void {
        if (self.vtable.emit_batch) |func| {
            try func(self.context, rows);
            return;
        }

        for (rows) |row| {
            try self.vtable.emit(self.context, &row);
        }
    }

    pub fn flush(self: Sink) Error!void {
        if (self.vtable.flush) |func| {
            try func(self.context);
        }
    }

    pub fn deinit(self: Sink, allocator: std.mem.Allocator) void {
        self.vtable.deinit(self.context, allocator);
    }
};

pub const VTable = struct {
    emit: *const fn (context: *anyopaque, row: *const sql_runtime.Row) Error!void,
    emit_batch: ?*const fn (context: *anyopaque, rows: []const sql_runtime.Row) Error!void = null,
    flush: ?*const fn (context: *anyopaque) Error!void = null,
    deinit: *const fn (context: *anyopaque, allocator: std.mem.Allocator) void,
};

const NullContext = struct {};

fn nullEmit(_: *anyopaque, _: *const sql_runtime.Row) Error!void {
    return;
}

fn nullFlush(_: *anyopaque) Error!void {
    return;
}

fn nullDeinit(context: *anyopaque, allocator: std.mem.Allocator) void {
    const typed: *NullContext = @ptrCast(@alignCast(context));
    allocator.destroy(typed);
}

const null_vtable = VTable{
    .emit = nullEmit,
    .emit_batch = null,
    .flush = nullFlush,
    .deinit = nullDeinit,
};

pub fn nullSink(allocator: std.mem.Allocator) Error!Sink {
    const ctx = try allocator.create(NullContext);
    ctx.* = .{};
    return Sink{
        .context = ctx,
        .vtable = &null_vtable,
    };
}
