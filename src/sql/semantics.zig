const std = @import("std");
const ast = @import("ast.zig");

const ManagedList = std.array_list.Managed;
const HashMap = std.HashMap;
const Fnv64 = std.hash.Fnv1a_64;

const ColumnUsageContext = struct {
    pub fn hash(_: @This(), usage: ColumnUsage) u64 {
        var hasher = Fnv64.init();
        const marker: u8 = if (usage.table == null) 0 else 1;
        hasher.update(&[_]u8{marker});
        if (usage.table) |table_name| {
            hasher.update(table_name);
        }
        hasher.update(&[_]u8{0xff});
        hasher.update(usage.name);
        return hasher.final();
    }

    pub fn eql(_: @This(), a: ColumnUsage, b: ColumnUsage) bool {
        return equalOpt(a.table, b.table) and equalSlice(a.name, b.name);
    }
};

const FunctionUsageContext = struct {
    pub fn hash(_: @This(), usage: FunctionUsage) u64 {
        var hasher = Fnv64.init();
        hasher.update(usage.name);
        const count = @as(u64, usage.arg_count);
        const bytes = std.mem.asBytes(&count);
        hasher.update(bytes);
        return hasher.final();
    }

    pub fn eql(_: @This(), a: FunctionUsage, b: FunctionUsage) bool {
        return equalSlice(a.name, b.name) and a.arg_count == b.arg_count;
    }
};

const ColumnSet = HashMap(ColumnUsage, void, ColumnUsageContext, std.hash_map.default_max_load_percentage);
const FunctionSet = HashMap(FunctionUsage, void, FunctionUsageContext, std.hash_map.default_max_load_percentage);

const CollectContext = struct {
    columns: *ManagedList(ColumnUsage),
    column_set: *ColumnSet,
    functions: *ManagedList(FunctionUsage),
    function_set: *FunctionSet,

    fn addColumn(self: *CollectContext, usage: ColumnUsage) !void {
        const entry = try self.column_set.getOrPut(usage);
        if (!entry.found_existing) {
            entry.value_ptr.* = {};
            try self.columns.append(usage);
        }
    }

    fn addFunction(self: *CollectContext, usage: FunctionUsage) !void {
        const entry = try self.function_set.getOrPut(usage);
        if (!entry.found_existing) {
            entry.value_ptr.* = {};
            try self.functions.append(usage);
        }
    }
};

pub const ColumnUsage = struct {
    table: ?[]const u8,
    name: []const u8,
};

pub const FunctionUsage = struct {
    name: []const u8,
    arg_count: usize,
};

pub const Analysis = struct {
    columns: []const ColumnUsage,
    functions: []const FunctionUsage,

    pub fn deinit(self: Analysis, allocator: std.mem.Allocator) void {
        allocator.free(self.columns);
        allocator.free(self.functions);
    }
};

pub fn analyzeSelect(allocator: std.mem.Allocator, stmt: *const ast.SelectStatement) !Analysis {
    var column_builder = ManagedList(ColumnUsage).init(allocator);
    defer column_builder.deinit();
    var function_builder = ManagedList(FunctionUsage).init(allocator);
    defer function_builder.deinit();

    var column_set = ColumnSet.init(allocator);
    defer column_set.deinit();
    var function_set = FunctionSet.init(allocator);
    defer function_set.deinit();

    var ctx = CollectContext{
        .columns = &column_builder,
        .column_set = &column_set,
        .functions = &function_builder,
        .function_set = &function_set,
    };

    for (stmt.projection) |item| {
        switch (item.kind) {
            .expression => |expr| try collectExpr(&ctx, expr),
            .star => {},
        }
    }

    if (stmt.selection) |expr| {
        try collectExpr(&ctx, expr);
    }

    if (stmt.having) |expr| {
        try collectExpr(&ctx, expr);
    }

    for (stmt.group_by) |expr| {
        try collectExpr(&ctx, expr);
    }

    if (stmt.window) |window_clause| {
        try collectExpr(&ctx, window_clause.timestamp);
        if (window_clause.size) |expr| try collectExpr(&ctx, expr);
        if (window_clause.slide) |expr| try collectExpr(&ctx, expr);
        if (window_clause.gap) |expr| try collectExpr(&ctx, expr);
        if (window_clause.watermark) |expr| try collectExpr(&ctx, expr);
        if (window_clause.allowed_lateness) |expr| try collectExpr(&ctx, expr);
    }

    for (stmt.order_by) |item| {
        try collectExpr(&ctx, item.expr);
    }

    for (stmt.from) |table_expr| {
        for (table_expr.joins) |join| {
            switch (join.constraint) {
                .none => {},
                .on => |expr| try collectExpr(&ctx, expr),
                .using => |cols| for (cols) |c| {
                    const usage = ColumnUsage{ .table = null, .name = c.canonical };
                    try ctx.addColumn(usage);
                },
            }
        }
    }

    const columns_slice = try column_builder.toOwnedSlice();
    const functions_slice = try function_builder.toOwnedSlice();

    return Analysis{
        .columns = columns_slice,
        .functions = functions_slice,
    };
}

fn collectExpr(ctx: *CollectContext, expr: *const ast.Expression) !void {
    switch (expr.*) {
        .column => |col| {
            const usage = ColumnUsage{
                .table = if (col.table) |t| t.canonical else null,
                .name = col.name.canonical,
            };
            try ctx.addColumn(usage);
        },
        .literal => {},
        .binary => |bin| {
            try collectExpr(ctx, bin.left);
            try collectExpr(ctx, bin.right);
        },
        .unary => |u| try collectExpr(ctx, u.operand),
        .function_call => |call| {
            const usage = FunctionUsage{
                .name = call.name.canonical,
                .arg_count = call.arguments.len,
            };
            try ctx.addFunction(usage);
            for (call.arguments) |arg| {
                try collectExpr(ctx, arg);
            }
        },
        .star => {},
    }
}

fn equalOpt(a: ?[]const u8, b: ?[]const u8) bool {
    if (a) |lhs| {
        if (b) |rhs| {
            return std.mem.eql(u8, lhs, rhs);
        }
        return false;
    }
    return b == null;
}

fn equalSlice(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

// Tests ---------------------------------------------------------------------

test "collect columns and functions" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const arena_alloc = arena_inst.allocator();

    const parser = @import("parser.zig");
    const stmt = try parser.parseSelect(arena_alloc, "SELECT ABS(value) AS v, message FROM logs WHERE level = 'info'");

    const gpa = std.testing.allocator;
    const analysis = try analyzeSelect(gpa, stmt);
    defer analysis.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 3), analysis.columns.len);
    try std.testing.expectEqual(@as(usize, 1), analysis.functions.len);

    try std.testing.expect(std.mem.eql(u8, analysis.functions[0].name, "abs"));
}
