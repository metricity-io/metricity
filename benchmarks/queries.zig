const std = @import("std");
const metricity = @import("metricity");

pub const QueryMode = enum { parse, format };

pub const QuerySource = union(enum) {
    literal: []const u8,
    generated: *const fn (std.mem.Allocator) anyerror![]const u8,
};

pub const BenchmarkQuery = struct {
    name: []const u8,
    mode: QueryMode = .parse,
    source: QuerySource,
    iterations: usize = 10_000,
    warmup_iterations: usize = 500,

    pub fn materialize(self: BenchmarkQuery, allocator: std.mem.Allocator) !MaterializedSql {
        return switch (self.source) {
            .literal => |text| MaterializedSql{ .text = text, .owned = false },
            .generated => |func| .{ .text = try func(allocator), .owned = true },
        };
    }
};

pub const MaterializedSql = struct {
    text: []const u8,
    owned: bool,
};

fn largeStatementGenerator(allocator: std.mem.Allocator) ![]const u8 {
    const count: usize = 1_000;

    var expr_buffer = std.array_list.Managed(u8).init(allocator);
    errdefer expr_buffer.deinit();
    var tables_buffer = std.array_list.Managed(u8).init(allocator);
    errdefer tables_buffer.deinit();
    var where_buffer = std.array_list.Managed(u8).init(allocator);
    errdefer where_buffer.deinit();
    var order_buffer = std.array_list.Managed(u8).init(allocator);
    errdefer order_buffer.deinit();

    var i: usize = 0;
    while (i < count) : (i += 1) {
        if (i != 0) {
            try expr_buffer.appendSlice(", ");
            try tables_buffer.appendSlice(" JOIN ");
            try where_buffer.appendSlice(" OR ");
            try order_buffer.appendSlice(", ");
        }
        try expr_buffer.writer().print("FN_{d}(COL_{d})", .{ i, i });
        try tables_buffer.writer().print("TABLE_{d}", .{i});
        try where_buffer.writer().print("COL_{d} = {d}", .{ i, i });
        try order_buffer.writer().print("COL_{d} DESC", .{i});
    }

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT {s} FROM {s} WHERE {s} ORDER BY {s}",
        .{ expr_buffer.items, tables_buffer.items, where_buffer.items, order_buffer.items },
    );

    expr_buffer.deinit();
    tables_buffer.deinit();
    where_buffer.deinit();
    order_buffer.deinit();

    return sql;
}

pub const queries = [_]BenchmarkQuery{
    .{
        .name = "sqlparser_select",
        .mode = .parse,
        .source = .{ .literal = "SELECT * FROM my_table WHERE 1 = 1" },
    },
    .{
        .name = "sqlparser_with_select",
        .mode = .parse,
        .source = .{ .literal = "WITH derived AS (SELECT MAX(a) AS max_a, COUNT(b) AS b_num, user_id FROM MY_TABLE GROUP BY user_id) " ++
            "SELECT * FROM my_table LEFT JOIN derived USING (user_id)" },
    },
    .{
        .name = "parse_large_statement",
        .mode = .parse,
        .source = .{ .generated = &largeStatementGenerator },
        .iterations = 200,
        .warmup_iterations = 20,
    },
    .{
        .name = "format_large_statement",
        .mode = .format,
        .source = .{ .generated = &largeStatementGenerator },
        .iterations = 200,
        .warmup_iterations = 20,
    },
    // Legacy micro benchmarks retained for quick checks.
    .{
        .name = "select_literal",
        .mode = .parse,
        .source = .{ .literal = "SELECT 1 FROM logs" },
    },
    .{
        .name = "simple_filter",
        .mode = .parse,
        .source = .{ .literal = "SELECT message FROM logs WHERE level = 'info'" },
    },
    .{
        .name = "arithmetic_projection",
        .mode = .parse,
        .source = .{ .literal = "SELECT (value + 7) * 3 AS score, message FROM metrics WHERE value >= 10" },
    },
};

test "benchmark queries parse and format" {
    inline for (queries) |item| {
        var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena_inst.deinit();
        const arena_alloc = arena_inst.allocator();

        const materialized = try item.materialize(std.testing.allocator);
        defer if (materialized.owned) std.testing.allocator.free(materialized.text);

        const stmt = try metricity.sql.parser.parseSelect(arena_alloc, materialized.text);
        try std.testing.expect(stmt.projection.len > 0);

        if (item.mode == .format) {
            const formatted = try metricity.sql.formatter.formatSelect(std.testing.allocator, stmt);
            defer std.testing.allocator.free(formatted);
        }
    }
}
