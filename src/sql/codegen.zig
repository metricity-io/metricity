const std = @import("std");
const ast = @import("ast.zig");

const CountingWriter = struct {
    bytes_written: usize = 0,

    pub fn writeAll(self: *CountingWriter, bytes: []const u8) !void {
        self.bytes_written += bytes.len;
    }

    pub fn writeByte(self: *CountingWriter, byte: u8) !void {
        _ = byte;
        self.bytes_written += 1;
    }

    pub fn print(self: *CountingWriter, comptime fmt: []const u8, args: anytype) !void {
        const len = std.fmt.count(fmt, args);
        self.bytes_written += len;
    }
};

pub const GeneratedExpression = struct {
    alias: ?[]const u8,
    code: []const u8,
};

pub const GeneratedSelect = struct {
    expressions: []const GeneratedExpression,

    pub fn deinit(self: GeneratedSelect, allocator: std.mem.Allocator) void {
        for (self.expressions) |expr| {
            allocator.free(expr.code);
        }
        allocator.free(self.expressions);
    }
};

fn renderExpression(allocator: std.mem.Allocator, expr: *const ast.Expression) ![]const u8 {
    var counter = CountingWriter{};
    try emitExpression(expr, &counter);
    const storage = try allocator.alloc(u8, counter.bytes_written);
    errdefer allocator.free(storage);

    var stream = std.io.fixedBufferStream(storage);
    try emitExpression(expr, stream.writer());
    return stream.getWritten();
}

pub fn generateExpressions(allocator: std.mem.Allocator, stmt: *const ast.SelectStatement) !GeneratedSelect {
    var expressions_builder = std.array_list.Managed(GeneratedExpression).init(allocator);
    defer expressions_builder.deinit();

    for (stmt.projection) |item| {
        var expr_ptr: ?*const ast.Expression = null;
        switch (item.kind) {
            .expression => |expr| expr_ptr = expr,
            .star => continue,
        }
        const expr = expr_ptr.?;
        const code_buf = try renderExpression(allocator, expr);
        const alias = if (item.alias) |alias_ident| alias_ident.text() else null;
        const gen = GeneratedExpression{
            .alias = alias,
            .code = code_buf,
        };
        try expressions_builder.append(gen);
    }

    const expressions_slice = try expressions_builder.toOwnedSlice();

    return GeneratedSelect{ .expressions = expressions_slice };
}

fn emitExpression(expr: *const ast.Expression, writer: anytype) !void {
    switch (expr.*) {
        .column => |col| try emitColumn(col, writer),
        .literal => |lit| try emitLiteral(lit, writer),
        .binary => |bin| {
            try writer.writeByte('(');
            try emitExpression(bin.left, writer);
            try writer.writeByte(' ');
            try writer.writeAll(operatorToken(bin.op));
            try writer.writeByte(' ');
            try emitExpression(bin.right, writer);
            try writer.writeByte(')');
        },
        .unary => |u| {
            try writer.writeByte('(');
            try writer.writeAll(unaryToken(u.op));
            try writer.writeByte(' ');
            try emitExpression(u.operand, writer);
            try writer.writeByte(')');
        },
        .function_call => |call| {
            if (call.distinct) return error.UnsupportedDistinct;
            try writer.writeAll("@call(.auto, funcs.");
            try writer.writeAll(call.name.text());
            try writer.writeAll(", .{");
            for (call.arguments, 0..) |arg, idx| {
                if (idx != 0) {
                    try writer.writeAll(", ");
                }
                try emitExpression(arg, writer);
            }
            try writer.writeAll("})");
        },
        .star => |s| {
            if (s.qualifier) |qual| {
                try writer.writeAll(qual.text());
                try writer.writeAll(".*");
            } else {
                try writer.writeByte('*');
            }
        },
    }
}

fn emitColumn(col: ast.ColumnRef, writer: anytype) !void {
    _ = col.table; // currently unused
    try writer.writeAll("event.");
    try writer.writeAll(col.name.text());
}

fn emitLiteral(lit: ast.Literal, writer: anytype) !void {
    switch (lit.value) {
        .integer => |value| try writer.print("{d}", .{value}),
        .float => |text| try writer.writeAll(text),
        .string => |text| try emitStringLiteral(text, writer),
        .boolean => |b| try writer.writeAll(if (b) "true" else "false"),
        .null => try writer.writeAll("null"),
    }
}

fn emitStringLiteral(value: []const u8, writer: anytype) !void {
    try writer.writeByte('"');
    for (value) |ch| {
        switch (ch) {
            '\\' => try writer.writeAll("\\\\"),
            '"' => try writer.writeAll("\\\""),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => try writer.writeByte(ch),
        }
    }
    try writer.writeByte('"');
}

fn operatorToken(op: ast.BinaryOperator) []const u8 {
    return switch (op) {
        .add => "+",
        .subtract => "-",
        .multiply => "*",
        .divide => "/",
        .equal => "==",
        .not_equal => "!=",
        .less => "<",
        .less_equal => "<=",
        .greater => ">",
        .greater_equal => ">=",
        .logical_and => "and",
        .logical_or => "or",
    };
}

fn unaryToken(op: ast.UnaryOperator) []const u8 {
    return switch (op) {
        .plus => "+",
        .minus => "-",
        .not => "not",
    };
}

// Tests ---------------------------------------------------------------------

test "generate expressions render valid zig" {
    const parser = @import("parser.zig");
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const arena_alloc = arena_inst.allocator();

    const stmt = try parser.parseSelect(arena_alloc, "SELECT value + 1 AS next_value FROM metrics WHERE level = 'info'");

    const gpa = std.testing.allocator;
    const generated = try generateExpressions(gpa, stmt);
    defer generated.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 1), generated.expressions.len);
    try std.testing.expect(std.mem.eql(u8, generated.expressions[0].code, "(event.value + 1)"));

    const snippet = try std.fmt.allocPrint(gpa, "const std = @import(\"std\");\nfn demo(event: anytype, funcs: anytype) void {{ const _ = {s}; }}", .{generated.expressions[0].code});
    defer gpa.free(snippet);

    const snippet_z = try std.mem.concatWithSentinel(gpa, u8, &.{snippet}, 0);
    defer gpa.free(snippet_z);

    var tree = try std.zig.Ast.parse(gpa, snippet_z, .zig);
    defer tree.deinit(gpa);
}

test "generate expressions preserve float literal text" {
    const parser = @import("parser.zig");
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const arena_alloc = arena_inst.allocator();

    const stmt = try parser.parseSelect(arena_alloc, "SELECT .5 AS half, 1e+06 FROM metrics");

    const gpa = std.testing.allocator;
    const generated = try generateExpressions(gpa, stmt);
    defer generated.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 2), generated.expressions.len);
    try std.testing.expect(std.mem.eql(u8, generated.expressions[0].code, ".5"));
    try std.testing.expect(std.mem.eql(u8, generated.expressions[1].code, "1e+06"));
}

test "generate expressions rejects distinct calls" {
    const parser = @import("parser.zig");
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const arena_alloc = arena_inst.allocator();

    const stmt = try parser.parseSelect(arena_alloc, "SELECT COUNT(DISTINCT user_id) FROM events");

    const gpa = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedDistinct, generateExpressions(gpa, stmt));
}
