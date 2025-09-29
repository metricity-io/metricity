const std = @import("std");
const ast = @import("ast.zig");
const lex = @import("lexer.zig");
const meta = std.meta;

const binary_operator_count = meta.fields(ast.BinaryOperator).len;

pub const BinaryToken = union(enum) {
    symbol: lex.Symbol,
    keyword: lex.Keyword,
};

pub const BinaryOperatorSpec = struct {
    token: BinaryToken,
    operator: ast.BinaryOperator,
    precedence: u8,
};

pub const binary_operator_specs = [_]BinaryOperatorSpec{
    .{ .token = .{ .symbol = .plus }, .operator = .add, .precedence = 40 },
    .{ .token = .{ .symbol = .minus }, .operator = .subtract, .precedence = 40 },
    .{ .token = .{ .symbol = .star }, .operator = .multiply, .precedence = 50 },
    .{ .token = .{ .symbol = .slash }, .operator = .divide, .precedence = 50 },
    .{ .token = .{ .symbol = .equal }, .operator = .equal, .precedence = 30 },
    .{ .token = .{ .symbol = .not_equal }, .operator = .not_equal, .precedence = 30 },
    .{ .token = .{ .symbol = .less }, .operator = .less, .precedence = 30 },
    .{ .token = .{ .symbol = .less_equal }, .operator = .less_equal, .precedence = 30 },
    .{ .token = .{ .symbol = .greater }, .operator = .greater, .precedence = 30 },
    .{ .token = .{ .symbol = .greater_equal }, .operator = .greater_equal, .precedence = 30 },
    .{ .token = .{ .keyword = .And }, .operator = .logical_and, .precedence = 20 },
    .{ .token = .{ .keyword = .Or }, .operator = .logical_or, .precedence = 10 },
};

pub fn findBinaryOperator(token: lex.Token.Kind) ?BinaryOperatorSpec {
    inline for (binary_operator_specs) |spec| {
        switch (spec.token) {
            .symbol => |sym| switch (token) {
                .symbol => |current_sym| if (current_sym == sym) return spec,
                else => {},
            },
            .keyword => |kw| switch (token) {
                .keyword => |current_kw| if (current_kw == kw) return spec,
                else => {},
            },
        }
    }
    return null;
}

pub fn precedenceForOperator(op: ast.BinaryOperator) u8 {
    return precedence_table[@intFromEnum(op)];
}

const precedence_table = initPrecedenceTable();

fn initPrecedenceTable() [binary_operator_count]u8 {
    var table: [binary_operator_count]u8 = undefined;
    var idx: usize = 0;
    while (idx < table.len) : (idx += 1) {
        table[idx] = 0;
    }
    for (binary_operator_specs) |spec| {
        const op_index = @intFromEnum(spec.operator);
        table[op_index] = spec.precedence;
    }
    return table;
}

comptime {
    var seen = [_]bool{false} ** binary_operator_count;
    for (binary_operator_specs) |spec| {
        const op_index = @intFromEnum(spec.operator);
        std.debug.assert(!seen[op_index]);
        seen[op_index] = true;
    }
    for (seen) |flag| {
        std.debug.assert(flag);
    }
}
