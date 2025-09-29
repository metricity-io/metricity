const std = @import("std");
const metricity = @import("metricity");
const bench_queries = @import("queries.zig");

const Config = struct {
    iterations_override: ?usize = null,
    warmup_override: ?usize = null,
    filters: []const []const u8 = &[_][]const u8{},
    list_only: bool = false,
    reuse_arena: bool = true,
};

const BenchmarkResult = struct {
    name: []const u8,
    iterations: usize,
    warmup_iterations: usize,
    elapsed_ns: u64,

    fn nsPerIter(self: BenchmarkResult) f128 {
        return if (self.iterations == 0)
            0
        else
            @as(f128, @floatFromInt(self.elapsed_ns)) / @as(f128, @floatFromInt(self.iterations));
    }

    fn throughput(self: BenchmarkResult) f128 {
        if (self.elapsed_ns == 0) return 0;
        const elapsed = @as(f128, @floatFromInt(self.elapsed_ns));
        return @as(f128, @floatFromInt(self.iterations)) * @as(f128, @floatFromInt(std.time.ns_per_s)) / elapsed;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const plain_args = try allocator.alloc([]const u8, args.len);
    defer allocator.free(plain_args);
    for (args, 0..) |arg, idx| {
        plain_args[idx] = std.mem.sliceTo(arg, 0);
    }

    var config = try parseConfig(allocator, plain_args);
    defer freeConfig(allocator, &config);

    if (config.list_only) {
        std.debug.print("Available queries:\n", .{});
        for (bench_queries.queries) |query| {
            std.debug.print(" - {s}\n", .{query.name});
        }
        return;
    }

    const selected = try selectQueries(allocator, config);
    defer allocator.free(selected);

    if (selected.len == 0) {
        return error.NoQueriesSelected;
    }

    std.debug.print("name,iterations,warmup,elapsed_ms,ns_per_iter,throughput_ops_per_sec\n", .{});

    for (selected) |entry| {
        const result = try runBenchmark(allocator, entry);
        const ns_per_iter = result.nsPerIter();
        const elapsed_ms = @as(f128, @floatFromInt(result.elapsed_ns)) / 1_000_000.0;
        std.debug.print(
            "{s},{d},{d},{d:.3},{d:.1},{d:.1}\n",
            .{
                result.name,
                result.iterations,
                result.warmup_iterations,
                elapsed_ms,
                ns_per_iter,
                result.throughput(),
            },
        );
    }
}

fn runBenchmark(allocator: std.mem.Allocator, query: SelectedQuery) !BenchmarkResult {
    defer if (query.owns_sql) allocator.free(query.sql);

    return switch (query.mode) {
        .parse => try runParseBenchmark(allocator, query),
        .format => try runFormatBenchmark(allocator, query),
    };
}

fn runLoop(allocator: std.mem.Allocator, sql_text: []const u8, iterations: usize, reuse_arena: bool) !void {
    if (reuse_arena) {
        var arena_inst = std.heap.ArenaAllocator.init(allocator);
        defer arena_inst.deinit();

        var i: usize = 0;
        while (i < iterations) : (i += 1) {
            _ = try metricity.sql.parser.parseSelect(arena_inst.allocator(), sql_text);
            _ = arena_inst.reset(.retain_capacity);
        }
    } else {
        var i: usize = 0;
        while (i < iterations) : (i += 1) {
            var arena_inst = std.heap.ArenaAllocator.init(allocator);
            defer arena_inst.deinit();
            _ = try metricity.sql.parser.parseSelect(arena_inst.allocator(), sql_text);
        }
    }
}

fn runParseBenchmark(allocator: std.mem.Allocator, query: SelectedQuery) !BenchmarkResult {
    try runLoop(allocator, query.sql, query.warmup_iterations, query.reuse_arena);

    var timer = try std.time.Timer.start();
    try runLoop(allocator, query.sql, query.iterations, query.reuse_arena);
    const elapsed_ns = timer.read();

    return BenchmarkResult{
        .name = query.name,
        .iterations = query.iterations,
        .warmup_iterations = query.warmup_iterations,
        .elapsed_ns = elapsed_ns,
    };
}

fn runFormatBenchmark(allocator: std.mem.Allocator, query: SelectedQuery) !BenchmarkResult {
    var arena_inst = std.heap.ArenaAllocator.init(allocator);
    defer arena_inst.deinit();

    const stmt = try metricity.sql.parser.parseSelect(arena_inst.allocator(), query.sql);

    var i: usize = 0;
    while (i < query.warmup_iterations) : (i += 1) {
        const formatted = try metricity.sql.formatter.formatSelect(allocator, stmt);
        allocator.free(formatted);
    }

    var timer = try std.time.Timer.start();
    i = 0;
    while (i < query.iterations) : (i += 1) {
        const formatted = try metricity.sql.formatter.formatSelect(allocator, stmt);
        allocator.free(formatted);
    }
    const elapsed_ns = timer.read();

    return BenchmarkResult{
        .name = query.name,
        .iterations = query.iterations,
        .warmup_iterations = query.warmup_iterations,
        .elapsed_ns = elapsed_ns,
    };
}

const SelectedQuery = struct {
    name: []const u8,
    sql: []const u8,
    iterations: usize,
    warmup_iterations: usize,
    reuse_arena: bool,
    mode: bench_queries.QueryMode,
    owns_sql: bool,
};

fn selectQueries(allocator: std.mem.Allocator, config: Config) ![]SelectedQuery {
    var list = std.array_list.Managed(SelectedQuery).init(allocator);
    errdefer {
        for (list.items) |entry| {
            if (entry.owns_sql) allocator.free(entry.sql);
        }
        list.deinit();
    }

    for (bench_queries.queries) |query| {
        if (config.filters.len > 0 and !matchesFilter(query.name, config.filters)) continue;
        const iterations = config.iterations_override orelse query.iterations;
        const warmup_iterations = config.warmup_override orelse query.warmup_iterations;
        const materialized = try query.materialize(allocator);
        try list.append(.{
            .name = query.name,
            .sql = materialized.text,
            .iterations = iterations,
            .warmup_iterations = warmup_iterations,
            .reuse_arena = config.reuse_arena,
            .mode = query.mode,
            .owns_sql = materialized.owned,
        });
    }

    const owned = try list.toOwnedSlice();
    list.deinit();
    return owned;
}

fn matchesFilter(name: []const u8, filters: []const []const u8) bool {
    for (filters) |f| {
        if (std.ascii.eqlIgnoreCase(name, f)) return true;
    }
    return false;
}

fn parseConfig(allocator: std.mem.Allocator, args: [][]const u8) !Config {
    var config = Config{};
    var filters = std.array_list.Managed([]const u8).init(allocator);
    errdefer {
        for (filters.items) |f| allocator.free(f);
        filters.deinit();
    }

    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--iterations")) {
            index += 1;
            if (index >= args.len) return error.MissingIterationsValue;
            const value = args[index];
            config.iterations_override = try parseUsize(value);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            index += 1;
            if (index >= args.len) return error.MissingWarmupValue;
            const value = args[index];
            config.warmup_override = try parseUsize(value);
        } else if (std.mem.eql(u8, arg, "--query")) {
            index += 1;
            if (index >= args.len) return error.MissingQueryValue;
            const value = args[index];
            const copy = try allocator.dupe(u8, value);
            try filters.append(copy);
        } else if (std.mem.eql(u8, arg, "--list")) {
            config.list_only = true;
        } else if (std.mem.eql(u8, arg, "--no-arena-reuse")) {
            config.reuse_arena = false;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        } else {
            std.debug.print("Unknown argument: {s}\n", .{arg});
            return error.UnknownArgument;
        }
    }

    if (filters.items.len > 0) {
        const owned = try filters.toOwnedSlice();
        filters.deinit();
        config.filters = owned;
    } else {
        filters.deinit();
    }

    return config;
}

fn freeConfig(allocator: std.mem.Allocator, config: *Config) void {
    if (config.filters.len == 0) return;
    for (config.filters) |f| allocator.free(f);
    allocator.free(config.filters);
    config.filters = &[_][]const u8{};
}

fn parseUsize(text: []const u8) !usize {
    return std.fmt.parseInt(usize, text, 10);
}

fn printUsage() void {
    std.debug.print(
        "Usage: zig build bench -- [--iterations N] [--warmup N] [--query NAME]* [--list]\n",
        .{},
    );
    std.debug.print("      [--no-arena-reuse]\n", .{});
}
