const std = @import("std");
const config_mod = @import("../config/mod.zig");
const sink_iface = @import("../pipeline/sink.zig");
const sql_runtime = @import("../sql/runtime.zig");
const event_mod = @import("source").event;

pub fn build(allocator: std.mem.Allocator, cfg: config_mod.ConsoleSink) sink_iface.Error!sink_iface.Sink {
    const file = switch (cfg.target) {
        .stdout => std.fs.File.stdout(),
        .stderr => std.fs.File.stderr(),
    };

    const context = try allocator.create(Context);
    errdefer allocator.destroy(context);

    context.* = .{
        .allocator = allocator,
        .file = file,
        .buffer = std.ArrayList(u8).empty,
    };
    errdefer context.buffer.deinit(allocator);

    try context.buffer.ensureTotalCapacity(allocator, 1024);

    return sink_iface.Sink{
        .context = context,
        .vtable = &vtable,
    };
}

const Context = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    buffer: std.ArrayList(u8),
};

fn emit(context_ptr: *anyopaque, row: *const sql_runtime.Row) sink_iface.Error!void {
    const context: *Context = @ptrCast(@alignCast(context_ptr));
    const rows = [_]sql_runtime.Row{row.*};
    try writeRows(context, rows[0..]);
}

fn emitBatch(context_ptr: *anyopaque, rows: []const sql_runtime.Row) sink_iface.Error!void {
    if (rows.len == 0) return;
    const context: *Context = @ptrCast(@alignCast(context_ptr));
    try writeRows(context, rows);
}

fn flush(context_ptr: *anyopaque) sink_iface.Error!void {
    const context: *Context = @ptrCast(@alignCast(context_ptr));
    try context.file.sync();
}

fn deinit(context_ptr: *anyopaque, allocator: std.mem.Allocator) void {
    const context: *Context = @ptrCast(@alignCast(context_ptr));
    context.buffer.deinit(context.allocator);
    allocator.destroy(context);
}

const vtable = sink_iface.VTable{
    .emit = emit,
    .emit_batch = emitBatch,
    .flush = flush,
    .deinit = deinit,
};

fn writeRows(context: *Context, rows: []const sql_runtime.Row) !void {
    context.buffer.clearRetainingCapacity();
    const writer = context.buffer.writer(context.allocator);
    for (rows) |row| {
        try writeRow(writer, &row);
    }
    try context.file.writeAll(context.buffer.items);
}

fn writeRow(writer: anytype, row: *const sql_runtime.Row) !void {
    try writer.writeByte('{');
    for (row.values, 0..) |entry, idx| {
        if (idx != 0) try writer.writeAll(", ");
        try writeJsonString(writer, entry.name);
        try writer.writeAll(": ");
        try writeJsonValue(writer, entry.value);
    }
    try writer.writeAll("}\n");
}

fn writeJsonString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |ch| {
        switch (ch) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            0x08 => try writer.writeAll("\\b"),
            0x0C => try writer.writeAll("\\f"),
            0x0A => try writer.writeAll("\\n"),
            0x0D => try writer.writeAll("\\r"),
            0x09 => try writer.writeAll("\\t"),
            else => {
                if (ch < 0x20 or ch == 0x7F) {
                    try writer.writeAll("\\u00");
                    const digits = "0123456789ABCDEF";
                    const hi = digits[(ch >> 4) & 0xF];
                    const lo = digits[ch & 0xF];
                    try writer.writeByte(hi);
                    try writer.writeByte(lo);
                } else {
                    try writer.writeByte(ch);
                }
            },
        }
    }
    try writer.writeByte('"');
}

fn writeJsonValue(writer: anytype, value: event_mod.Value) !void {
    switch (value) {
        .string => |text| try writeJsonString(writer, text),
        .integer => |n| try writer.print("{d}", .{n}),
        .float => |f| try formatFloat(writer, f),
        .boolean => |b| try writer.writeAll(if (b) "true" else "false"),
        .null => try writer.writeAll("null"),
    }
}

fn formatFloat(writer: anytype, value: f64) !void {
    var buffer: [std.fmt.float.min_buffer_size]u8 = undefined;
    const rendered = std.fmt.float.render(buffer[0..], value, .{ .mode = .scientific }) catch {
        return sink_iface.Error.EmitFailed;
    };
    try writer.writeAll(rendered);
}

const testing = std.testing;

fn mockRow(allocator: std.mem.Allocator) !sql_runtime.Row {
    var entries = try allocator.alloc(sql_runtime.ValueEntry, 2);
    entries[0] = .{ .name = "syslog_severity", .value = .{ .integer = 4 } };
    entries[1] = .{ .name = "message", .value = .{ .string = "hello" } };
    return sql_runtime.Row{ .allocator = allocator, .values = entries };
}

test "console sink formats row as json-like output" {
    const allocator = testing.allocator;
    var buffer: [128]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);

    var row = try mockRow(allocator);
    defer row.deinit();

    try writeRow(stream.writer(), &row);
    const written = stream.getWritten();
    try testing.expectEqualStrings("{\"syslog_severity\": 4, \"message\": \"hello\"}\n", written);
}

test "writeJsonString escapes control characters" {
    var buffer: [128]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);

    try writeJsonString(stream.writer(), "\x00\x1B\x7F");
    const written = stream.getWritten();
    try testing.expectEqualStrings("\"\\u0000\\u001B\\u007F\"", written);
}

test "writeJsonString escapes common sequences" {
    var buffer: [128]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);

    try writeJsonString(stream.writer(), "line1\nline2\t\"quote\"\\");
    const written = stream.getWritten();
    try testing.expectEqualStrings("\"line1\\nline2\\t\\\"quote\\\"\\\\\"", written);
}

test "emitBatch writes multiple rows using shared buffer" {
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();

    const allocator = testing.allocator;
    const file = try dir.dir.createFile("console.log", .{ .read = true, .truncate = true });

    var context = Context{
        .allocator = allocator,
        .file = file,
        .buffer = std.ArrayList(u8).empty,
    };
    defer context.buffer.deinit(allocator);

    var first = try mockRow(allocator);
    defer first.deinit();
    var second = try mockRow(allocator);
    defer second.deinit();

    const rows = [_]sql_runtime.Row{ first, second };
    try writeRows(&context, rows[0..]);
    try context.file.sync();
    context.file.close();

    const content = try dir.dir.readFileAlloc(allocator, "console.log", 1024);
    defer allocator.free(content);
    try testing.expectEqualStrings("{\"syslog_severity\": 4, \"message\": \"hello\"}\n{\"syslog_severity\": 4, \"message\": \"hello\"}\n", content);
}
