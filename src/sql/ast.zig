const std = @import("std");
const lex = @import("lexer.zig");

pub const Span = lex.Span;

pub const Identifier = struct {
    /// Original (unescaped) representation preserved for formatting/output.
    value: []const u8,
    /// Canonical lower-case representation used for lookups/comparisons.
    canonical: []const u8,
    /// Whether the identifier was quoted and therefore case-sensitive.
    quoted: bool = false,
    /// Source span covering the identifier token.
    span: Span,

    pub fn eql(self: Identifier, other: Identifier) bool {
        return std.mem.eql(u8, self.canonical, other.canonical);
    }

    pub fn isQuoted(self: Identifier) bool {
        return self.quoted;
    }

    pub fn text(self: Identifier) []const u8 {
        return self.value;
    }

    pub fn write(self: Identifier, writer: anytype) !void {
        if (self.quoted) {
            try writer.writeByte('"');
            for (self.value) |ch| {
                if (ch == '"') {
                    try writer.writeAll("\"\"");
                } else {
                    try writer.writeByte(ch);
                }
            }
            try writer.writeByte('"');
            return;
        }

        try writer.writeAll(self.value);
    }
};

pub const Literal = struct {
    span: Span,
    value: Value,

    pub const Value = union(enum) {
        integer: i64,
        /// Floating literals keep their textual form to preserve formatting.
        float: []const u8,
        string: []const u8,
        boolean: bool,
        null,
    };
};

pub const UnaryOperator = enum {
    plus,
    minus,
    not,
};

pub const BinaryOperator = enum {
    add,
    subtract,
    multiply,
    divide,
    equal,
    not_equal,
    less,
    less_equal,
    greater,
    greater_equal,
    logical_and,
    logical_or,
};

pub const ColumnRef = struct {
    table: ?Identifier = null,
    name: Identifier,
    span: Span,
};

pub const Star = struct {
    qualifier: ?Identifier = null,
    span: Span,
};

pub const UnaryExpr = struct {
    op: UnaryOperator,
    operand: *const Expression,
    span: Span,
};

pub const BinaryExpr = struct {
    op: BinaryOperator,
    left: *const Expression,
    right: *const Expression,
    span: Span,
};

pub const FunctionCall = struct {
    name: Identifier,
    arguments: []const *const Expression,
    distinct: bool = false,
    span: Span,
};

pub const Expression = union(enum) {
    column: ColumnRef,
    literal: Literal,
    binary: BinaryExpr,
    unary: UnaryExpr,
    function_call: FunctionCall,
    star: Star,
};

pub const SelectItemKind = union(enum) {
    expression: *const Expression,
    star: Star,
};

pub const SelectItem = struct {
    kind: SelectItemKind,
    alias: ?Identifier = null,
    span: Span,
};

pub const TableRef = struct {
    name: Identifier,
    alias: ?Identifier = null,
    span: Span,
};

pub const DerivedTable = struct {
    subquery: *const SelectStatement,
    alias: Identifier,
    span: Span,
};

pub const Relation = union(enum) {
    table: TableRef,
    derived: DerivedTable,
};

pub const JoinKind = enum {
    inner,
    left,
};

pub const JoinConstraint = union(enum) {
    none,
    on: *const Expression,
    using: []const Identifier,
};

pub const Join = struct {
    kind: JoinKind = .inner,
    relation: Relation,
    constraint: JoinConstraint = .none,
    span: Span,
};

pub const TableExpression = struct {
    relation: Relation,
    joins: []const Join = &[_]Join{},
    span: Span,
};

pub const SortDirection = enum { asc, desc };

pub const OrderByItem = struct {
    expr: *const Expression,
    direction: SortDirection = .asc,
    span: Span,
};

pub const CommonTableExpression = struct {
    name: Identifier,
    columns: []const Identifier = &[_]Identifier{},
    query: *const SelectStatement,
    span: Span,
};

pub const WithClause = struct {
    recursive: bool = false,
    ctes: []const CommonTableExpression,
    span: Span,
};

pub const SelectStatement = struct {
    with_clause: ?WithClause = null,
    distinct: bool = false,
    distinct_span: ?Span = null,
    projection: []const SelectItem,
    from: []const TableExpression = &[_]TableExpression{},
    selection: ?*const Expression = null,
    group_by: []const *const Expression = &[_]*const Expression{},
    having: ?*const Expression = null,
    order_by: []const OrderByItem = &[_]OrderByItem{},
    order_by_span: ?Span = null,
    span: Span,
};

pub const Query = union(enum) {
    select: *const SelectStatement,
};

pub fn expressionSpan(expr: *const Expression) Span {
    return switch (expr.*) {
        .column => |col| col.span,
        .literal => |lit| lit.span,
        .binary => |bin| bin.span,
        .unary => |un| un.span,
        .function_call => |call| call.span,
        .star => |star| star.span,
    };
}

pub fn relationSpan(rel: Relation) Span {
    return switch (rel) {
        .table => |table| table.span,
        .derived => |derived| derived.span,
    };
}

comptime {
    std.debug.assert(@intFromEnum(BinaryOperator.add) == 0);
    std.debug.assert(@intFromEnum(BinaryOperator.logical_or) == 11);
}

test "identifier equality ignores case" {
    const span = Span{ .start = 0, .end = 0 };
    const a = Identifier{ .value = "Foo", .canonical = "foo", .span = span };
    const b = Identifier{ .value = "foo", .canonical = "foo", .span = span };
    try std.testing.expect(a.eql(b));
}

test "build simple select statement" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    const dummy_span = Span{ .start = 0, .end = 0 };

    const col_expr = try allocator.create(Expression);
    col_expr.* = .{ .column = ColumnRef{
        .name = .{ .value = "message", .canonical = "message", .span = dummy_span },
        .span = dummy_span,
    } };

    const lit_expr = try allocator.create(Expression);
    lit_expr.* = .{ .literal = .{ .span = dummy_span, .value = .{ .string = "INFO" } } };

    const cmp_expr = try allocator.create(Expression);
    cmp_expr.* = .{ .binary = BinaryExpr{
        .op = .equal,
        .left = col_expr,
        .right = lit_expr,
        .span = dummy_span,
    } };

    const projection = try allocator.alloc(SelectItem, 1);
    projection[0] = .{
        .kind = .{ .expression = col_expr },
        .alias = null,
        .span = dummy_span,
    };

    const from_tables = try allocator.alloc(TableExpression, 1);
    from_tables[0] = .{
        .relation = .{ .table = .{
            .name = .{ .value = "logs", .canonical = "logs", .span = dummy_span },
            .span = dummy_span,
        } },
        .joins = &[_]Join{},
        .span = dummy_span,
    };

    const select = try allocator.create(SelectStatement);
    select.* = .{
        .projection = projection,
        .from = from_tables,
        .selection = cmp_expr,
        .span = dummy_span,
    };

    const query = Query{ .select = select };
    switch (query) {
        .select => |stmt| {
            try std.testing.expectEqual(@as(usize, 1), stmt.projection.len);
            try std.testing.expect(stmt.selection != null);
            try std.testing.expectEqual(@as(usize, 1), stmt.from.len);
        },
    }
}

test "identifier write emits quoting" {
    const dummy_span = Span{ .start = 0, .end = 0 };
    var buffer: [64]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    const writer = stream.writer();

    const quoted = Identifier{ .value = "A\"B", .canonical = "A\"B", .quoted = true, .span = dummy_span };
    try quoted.write(writer);
    try std.testing.expect(std.mem.eql(u8, stream.getWritten(), "\"A\"\"B\""));

    stream.reset();
    const plain = Identifier{ .value = "Foo", .canonical = "foo", .quoted = false, .span = dummy_span };
    try plain.write(writer);
    try std.testing.expect(std.mem.eql(u8, stream.getWritten(), "Foo"));
}
