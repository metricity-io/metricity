const std = @import("std");
const ast = @import("ast.zig");
const operators = @import("operators.zig");

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

pub fn formatSelect(allocator: std.mem.Allocator, stmt: *const ast.SelectStatement) anyerror![]const u8 {
    var buffer = std.array_list.Managed(u8).init(allocator);
    errdefer buffer.deinit();
    var counter = CountingWriter{};
    try formatSelectInto(&counter, stmt);
    try buffer.ensureTotalCapacity(counter.bytes_written);
    try formatSelectInto(buffer.writer(), stmt);
    return buffer.toOwnedSlice();
}

fn formatSelectInto(writer: anytype, stmt: *const ast.SelectStatement) anyerror!void {
    if (stmt.with_clause) |with_clause| {
        try writer.writeAll("WITH ");
        if (with_clause.recursive) try writer.writeAll("RECURSIVE ");
        for (with_clause.ctes, 0..) |cte, idx| {
            if (idx != 0) try writer.writeAll(", ");
            try cte.name.write(writer);
            if (cte.columns.len > 0) {
                try writer.writeByte('(');
                try formatIdentifierList(writer, cte.columns);
                try writer.writeByte(')');
            }
            try writer.writeAll(" AS (");
            try formatSelectInto(writer, cte.query);
            try writer.writeByte(')');
        }
        try writer.writeByte(' ');
    }

    try writer.writeAll("SELECT ");
    if (stmt.distinct) try writer.writeAll("DISTINCT ");
    for (stmt.projection, 0..) |item, idx| {
        if (idx != 0) try writer.writeAll(", ");
        try formatSelectItem(writer, item);
    }

    if (stmt.from.len > 0) {
        try writer.writeAll(" FROM ");
        for (stmt.from, 0..) |table_expr, idx| {
            if (idx != 0) try writer.writeAll(", ");
            try formatTableExpression(writer, table_expr);
        }
    }

    if (stmt.selection) |expr| {
        try writer.writeAll(" WHERE ");
        try formatExpression(writer, expr);
    }

    if (stmt.group_by.len > 0) {
        try writer.writeAll(" GROUP BY ");
        for (stmt.group_by, 0..) |expr, idx| {
            if (idx != 0) try writer.writeAll(", ");
            try formatExpression(writer, expr);
        }
    }

    if (stmt.having) |expr| {
        try writer.writeAll(" HAVING ");
        try formatExpression(writer, expr);
    }

    if (stmt.order_by.len > 0) {
        try writer.writeAll(" ORDER BY ");
        for (stmt.order_by, 0..) |item, idx| {
            if (idx != 0) try writer.writeAll(", ");
            try formatExpression(writer, item.expr);
            switch (item.direction) {
                .asc => {},
                .desc => try writer.writeAll(" DESC"),
            }
        }
    }
}

fn formatSelectItem(writer: anytype, item: ast.SelectItem) anyerror!void {
    switch (item.kind) {
        .star => |star| try formatStar(writer, star),
        .expression => |expr| try formatExpression(writer, expr),
    }
    if (item.alias) |alias| {
        try writer.writeAll(" AS ");
        try alias.write(writer);
    }
}

fn formatTableExpression(writer: anytype, table_expr: ast.TableExpression) anyerror!void {
    try formatRelation(writer, table_expr.relation);
    for (table_expr.joins) |join| {
        switch (join.kind) {
            .inner => try writer.writeAll(" JOIN "),
            .left => try writer.writeAll(" LEFT JOIN "),
        }
        try formatRelation(writer, join.relation);
        switch (join.constraint) {
            .none => {},
            .on => |expr| {
                try writer.writeAll(" ON ");
                try formatExpression(writer, expr);
            },
            .using => |cols| {
                try writer.writeAll(" USING (");
                try formatIdentifierList(writer, cols);
                try writer.writeByte(')');
            },
        }
    }
}

fn formatRelation(writer: anytype, relation: ast.Relation) anyerror!void {
    switch (relation) {
        .table => |table| {
            try table.name.write(writer);
            if (table.alias) |alias| {
                try writer.writeByte(' ');
                try alias.write(writer);
            }
        },
        .derived => |derived| {
            try writer.writeByte('(');
            try formatSelectInto(writer, derived.subquery);
            try writer.writeByte(')');
            try writer.writeByte(' ');
            try derived.alias.write(writer);
        },
    }
}

fn formatIdentifierList(writer: anytype, list: []const ast.Identifier) anyerror!void {
    for (list, 0..) |id, idx| {
        if (idx != 0) try writer.writeAll(", ");
        try id.write(writer);
    }
}

fn formatStar(writer: anytype, star: ast.Star) anyerror!void {
    if (star.qualifier) |qual| {
        try qual.write(writer);
        try writer.writeAll(".*");
    } else {
        try writer.writeByte('*');
    }
}

fn formatExpression(writer: anytype, expr: *const ast.Expression) anyerror!void {
    try formatExpressionWithPrecedence(writer, expr, 0, false);
}

fn formatExpressionWithPrecedence(
    writer: anytype,
    expr: *const ast.Expression,
    parent_prec: u8,
    is_right_child: bool,
) anyerror!void {
    switch (expr.*) {
        .column => |col| {
            if (col.table) |table| {
                try table.write(writer);
                try writer.writeByte('.');
            }
            try col.name.write(writer);
        },
        .literal => |lit| try formatLiteral(writer, lit),
        .binary => |bin| {
            const prec = operators.precedenceForOperator(bin.op);
            const needs_paren = prec < parent_prec or (is_right_child and prec == parent_prec);
            if (needs_paren) try writer.writeByte('(');
            try formatExpressionWithPrecedence(writer, bin.left, prec, false);
            try writer.writeByte(' ');
            try writer.writeAll(operatorToken(bin.op));
            try writer.writeByte(' ');
            try formatExpressionWithPrecedence(writer, bin.right, prec, true);
            if (needs_paren) try writer.writeByte(')');
        },
        .unary => |u| {
            const prec: u8 = 60;
            const needs_paren = prec < parent_prec;
            if (needs_paren) try writer.writeByte('(');
            switch (u.op) {
                .not => try writer.writeAll("NOT "),
                .plus => try writer.writeByte('+'),
                .minus => try writer.writeByte('-'),
            }
            try formatExpressionWithPrecedence(writer, u.operand, prec, false);
            if (needs_paren) try writer.writeByte(')');
        },
        .function_call => |call| {
            try call.name.write(writer);
            try writer.writeByte('(');
            if (call.distinct) try writer.writeAll("DISTINCT ");
            for (call.arguments, 0..) |arg, idx| {
                if (idx != 0) try writer.writeAll(", ");
                try formatExpressionWithPrecedence(writer, arg, 0, false);
            }
            try writer.writeByte(')');
        },
        .star => |star| try formatStar(writer, star),
    }
}

fn formatLiteral(writer: anytype, lit: ast.Literal) anyerror!void {
    switch (lit.value) {
        .integer => |value| try writer.print("{d}", .{value}),
        .float => |text| try writer.writeAll(text),
        .string => |text| {
            try writer.writeByte('\'');
            for (text) |ch| {
                if (ch == '\'') {
                    try writer.writeAll("''");
                } else {
                    try writer.writeByte(ch);
                }
            }
            try writer.writeByte('\'');
        },
        .boolean => |b| try writer.writeAll(if (b) "TRUE" else "FALSE"),
        .null => try writer.writeAll("NULL"),
    }
}

fn operatorToken(op: ast.BinaryOperator) []const u8 {
    return switch (op) {
        .add => "+",
        .subtract => "-",
        .multiply => "*",
        .divide => "/",
        .equal => "=",
        .not_equal => "!=",
        .less => "<",
        .less_equal => "<=",
        .greater => ">",
        .greater_equal => ">=",
        .logical_and => "AND",
        .logical_or => "OR",
    };
}

// Tests ---------------------------------------------------------------------

test "format simple select" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const parser = @import("parser.zig");
    const stmt = try parser.parseSelect(allocator, "SELECT message FROM logs WHERE level = 'info'");

    const gpa = std.testing.allocator;
    const sql = try formatSelect(gpa, stmt);
    defer gpa.free(sql);
    try std.testing.expect(std.mem.eql(u8, sql, "SELECT message FROM logs WHERE level = 'info'"));
}

test "format select distinct" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const parser = @import("parser.zig");
    const stmt = try parser.parseSelect(allocator, "SELECT DISTINCT message FROM logs");

    const gpa = std.testing.allocator;
    const sql = try formatSelect(gpa, stmt);
    defer gpa.free(sql);
    try std.testing.expect(std.mem.eql(u8, sql, "SELECT DISTINCT message FROM logs"));
}

test "format preserves float literal text" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const parser = @import("parser.zig");
    const stmt = try parser.parseSelect(allocator, "SELECT .5, 1., 1e+06 FROM numbers");

    const gpa = std.testing.allocator;
    const sql = try formatSelect(gpa, stmt);
    defer gpa.free(sql);
    try std.testing.expect(std.mem.eql(u8, sql, "SELECT .5, 1., 1e+06 FROM numbers"));
}

test "format cte with join" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const parser = @import("parser.zig");
    const sql = "WITH derived AS (SELECT MAX(a) AS max_a, COUNT(b) AS b_num, user_id FROM MY_TABLE GROUP BY user_id) SELECT * FROM my_table LEFT JOIN derived USING (user_id)";
    const stmt = try parser.parseSelect(allocator, sql);

    const gpa = std.testing.allocator;
    const formatted = try formatSelect(gpa, stmt);
    defer gpa.free(formatted);

    try std.testing.expect(std.mem.eql(u8, formatted, sql));
}

test "format quoted identifiers preserves quoting" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const allocator = arena_inst.allocator();

    const parser = @import("parser.zig");
    const stmt = try parser.parseSelect(allocator, "SELECT \"A\"\"B\" FROM \"T\"\"able\"");

    const gpa = std.testing.allocator;
    const formatted = try formatSelect(gpa, stmt);
    defer gpa.free(formatted);

    try std.testing.expect(std.mem.eql(u8, formatted, "SELECT \"A\"\"B\" FROM \"T\"\"able\""));
}

test "format survives scratch reset" {
    const parser = @import("parser.zig");
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    var scratch = parser.Scratch.init(std.testing.allocator);
    defer scratch.deinit();

    const stmt = try parser.parseSelectWithScratch(arena_inst.allocator(), &scratch, "SELECT message FROM logs WHERE level = 'info'");
    scratch.reset();

    const gpa = std.testing.allocator;
    const formatted = try formatSelect(gpa, stmt);
    defer gpa.free(formatted);
    try std.testing.expect(std.mem.eql(u8, formatted, "SELECT message FROM logs WHERE level = 'info'"));
}

test "format generated statement" {
    const gpa = std.testing.allocator;
    var arena_inst = std.heap.ArenaAllocator.init(gpa);
    defer arena_inst.deinit();

    const parser = @import("parser.zig");

    var expressions_buffer = std.array_list.Managed(u8).init(gpa);
    defer expressions_buffer.deinit();
    var tables_buffer = std.array_list.Managed(u8).init(gpa);
    defer tables_buffer.deinit();
    var where_buffer = std.array_list.Managed(u8).init(gpa);
    defer where_buffer.deinit();
    var order_buffer = std.array_list.Managed(u8).init(gpa);
    defer order_buffer.deinit();

    var i: usize = 0;
    const count: usize = 10;
    while (i < count) : (i += 1) {
        if (i != 0) {
            try expressions_buffer.appendSlice(", ");
            try tables_buffer.appendSlice(" JOIN ");
            try where_buffer.appendSlice(" OR ");
            try order_buffer.appendSlice(", ");
        }
        try expressions_buffer.writer().print("FN_{d}(COL_{d})", .{ i, i });
        try tables_buffer.writer().print("TABLE_{d}", .{i});
        try where_buffer.writer().print("COL_{d} = {d}", .{ i, i });
        try order_buffer.writer().print("COL_{d} DESC", .{i});
    }

    const large_sql = try std.fmt.allocPrint(
        gpa,
        "SELECT {s} FROM {s} WHERE {s} ORDER BY {s}",
        .{ expressions_buffer.items, tables_buffer.items, where_buffer.items, order_buffer.items },
    );
    defer gpa.free(large_sql);

    const stmt = try parser.parseSelect(arena_inst.allocator(), large_sql);
    const formatted = try formatSelect(gpa, stmt);
    defer gpa.free(formatted);

    try std.testing.expect(std.mem.eql(u8, formatted, large_sql));
}
