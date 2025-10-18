const std = @import("std");
const process = std.process;
const cli = @import("app/cli.zig");
const runtime = @import("app/runtime.zig");
const metricity = @import("metricity");
const config_parser = metricity.config_parser;
const config_mod = metricity.config;
const pipeline_mod = metricity.pipeline;
const collector_mod = metricity.collector;
const sql_mod = metricity.sql;
const sql_runtime = sql_mod.runtime;
const sql_parser = sql_mod.parser;
const sink_mod = pipeline_mod.sink;
const fs = std.fs;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const stdout_file = std.fs.File.stdout();
    const stderr_file = std.fs.File.stderr();

    var stdout_buffer: [1024]u8 = undefined;
    var stderr_buffer: [1024]u8 = undefined;
    var stdout_writer = stdout_file.writer(stdout_buffer[0..]);
    var stderr_writer = stderr_file.writer(stderr_buffer[0..]);

    const env = runtime.Environment{
        .allocator = allocator,
        .stdout = &stdout_writer.interface,
        .stderr = &stderr_writer.interface,
    };

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const parsed = cli.parse(args) catch |err| {
        try reportParseError(env.stderr, err);
        try env.stderr.print("\n{s}", .{cli.usage()});
        return;
    };

    try dispatch(env, parsed);
}

fn dispatch(env: runtime.Environment, parsed: cli.Parsed) !void {
    switch (parsed) {
        .help => {
            try env.stdout.print("{s}", .{cli.usage()});
        },
        .run => |options| {
            runtime.run(env, options) catch |err| {
                try handleRuntimeError(env, err);
                process.exit(1);
            };
        },
        .check => |options| {
            runtime.check(env, options) catch |err| {
                try handleRuntimeError(env, err);
                process.exit(1);
            };
        },
    }
}

fn reportParseError(writer: *std.io.Writer, err: cli.ParseError) !void {
    try writer.print("error: {s}\n", .{describeParseError(err)});
}

fn handleRuntimeError(env: runtime.Environment, err: runtime.Error) !void {
    const desc = describeRuntimeError(err);
    if (desc.detail) |detail| {
        try env.stderr.print("error: {s} ({s})\n", .{ desc.message, detail });
    } else {
        try env.stderr.print("error: {s}\n", .{desc.message});
    }
}

const RuntimeErrorDescription = struct {
    message: []const u8,
    detail: ?[]const u8 = null,
};

threadlocal var feature_gate_detail_buffer: [128]u8 = undefined;

fn formatFeatureGateDetail(diag: sql_runtime.FeatureGateDiagnostic) []const u8 {
    var stream = std.io.fixedBufferStream(&feature_gate_detail_buffer);
    stream.reset();
    const writer = stream.writer();
    const name = sql_runtime.featureGateName(diag.feature);
    writer.print("{s} unsupported (span {d}..{d})", .{ name, diag.span.start, diag.span.end }) catch {
        return "unsupported feature";
    };
    return stream.getWritten();
}

fn describeRuntimeError(err: runtime.Error) RuntimeErrorDescription {
    return switch (err) {
        std.mem.Allocator.Error.OutOfMemory => .{ .message = "allocator exhausted memory" },
        std.Thread.SpawnError.ThreadQuotaExceeded => .{ .message = "failed to spawn worker thread: thread quota exceeded" },
        std.Thread.SpawnError.SystemResources => .{ .message = "failed to spawn worker thread: insufficient system resources" },
        std.Thread.SpawnError.LockedMemoryLimitExceeded => .{ .message = "failed to spawn worker thread: locked memory limit exceeded" },
        config_parser.ParseError.InvalidSyntax => .{ .message = "invalid configuration syntax" },
        config_parser.ParseError.UnknownSection => .{ .message = "unknown configuration section" },
        config_parser.ParseError.DuplicateComponent => .{ .message = "duplicate component identifier" },
        config_parser.ParseError.DuplicateKey => .{ .message = "duplicate key in configuration table" },
        config_parser.ParseError.MissingType => .{ .message = "configuration entry is missing a type" },
        config_parser.ParseError.UnsupportedTransformType => .{ .message = "unsupported transform type" },
        config_parser.ParseError.UnsupportedSinkType => .{ .message = "unsupported sink type" },
        config_parser.ParseError.MissingInputs => .{ .message = "component is missing required inputs" },
        config_parser.ParseError.MissingQuery => .{ .message = "sql transform is missing a query" },
        config_parser.ParseError.InvalidValue => .{ .message = "invalid configuration value" },
        config_parser.ParseError.MissingSection => .{ .message = "configuration lacks required sections" },
        config_parser.ParseError.ConfigTooLarge => .{ .message = "configuration file exceeds size limit" },
        config_mod.ValidationError.SourceIdMismatch => .{ .message = "source id does not match configuration entry" },
        config_mod.ValidationError.UnknownComponent => .{ .message = "pipeline references unknown component" },
        config_mod.ValidationError.InvalidEdge => .{ .message = "invalid pipeline wiring" },
        config_mod.ValidationError.MissingSinks => .{ .message = "pipeline must define at least one sink" },
        config_mod.ValidationError.InvalidParallelism => .{ .message = "pipeline component declares zero parallelism" },
        config_mod.ValidationError.InvalidQueueCapacity => .{ .message = "pipeline queue capacity must be greater than zero" },
        config_mod.ValidationError.InvalidLimit => .{ .message = "pipeline limit configuration is invalid" },
        config_mod.ValidationError.InvalidShardCount => .{ .message = "transform shard count must be greater than zero" },
        config_mod.ValidationError.MissingShardKey => .{ .message = "sharded transform requires a shard key" },
        pipeline_mod.Error.CycleDetected => .{ .message = "pipeline topology contains cycles" },
        pipeline_mod.Error.UnknownSource => .{ .message = "collector produced batch for unknown source" },
        pipeline_mod.Error.ChannelClosed => .{ .message = "worker communication channel closed unexpectedly" },
        collector_mod.CollectorError.InvalidConfiguration => .{ .message = "collector: invalid configuration" },
        collector_mod.CollectorError.InitializationFailed => .{ .message = "collector: failed to initialise source" },
        collector_mod.CollectorError.StartFailed => .{ .message = "collector: failed to start source" },
        collector_mod.CollectorError.PollFailed => .{ .message = "collector: failed to poll source" },
        collector_mod.CollectorError.NotStarted => .{ .message = "collector: poll attempted before start" },
        collector_mod.CollectorError.Backpressure => .{ .message = "collector: backpressure encountered" },
        collector_mod.CollectorError.ShutdownFailed => .{ .message = "collector: failed to shutdown source" },
        collector_mod.CollectorError.UnsupportedSource => .{ .message = "collector: unsupported source type" },
        collector_mod.CollectorError.Unimplemented => .{ .message = "collector: feature not implemented" },
        sql_parser.ParseError.UnexpectedToken => .{ .message = "sql parser: unexpected token" },
        sql_parser.ParseError.ExpectedToken => .{ .message = "sql parser: expected token missing" },
        sql_parser.ParseError.InvalidNumber => .{ .message = "sql parser: invalid numeric literal" },
        sql_parser.ParseError.LexError => .{ .message = "sql parser: lexical analysis failed" },
        sql_runtime.Error.UnsupportedFeature => blk: {
            const diag = sql_runtime.takeFeatureGateDiagnostic();
            if (diag) |d| {
                break :blk .{
                    .message = "sql runtime: unsupported feature",
                    .detail = formatFeatureGateDetail(d),
                };
            }
            break :blk .{ .message = "sql runtime: unsupported feature" };
        },
        sql_runtime.Error.UnknownColumn => .{ .message = "sql runtime: unknown column" },
        sql_runtime.Error.TypeMismatch => .{ .message = "sql runtime: type mismatch" },
        sql_runtime.Error.UnsupportedFunction => .{ .message = "sql runtime: unsupported function" },
        sql_runtime.Error.DivideByZero => .{ .message = "sql runtime: divide by zero" },
        sink_mod.Error.EmitFailed => .{ .message = "sink: emit failed" },
        fs.File.OpenError.FileNotFound => .{ .message = "configuration file not found" },
        fs.File.OpenError.AccessDenied => .{ .message = "insufficient permissions to read configuration" },
        fs.File.ReadError.Unexpected => .{ .message = "failed to read configuration file", .detail = @errorName(err) },
        fs.File.ReadError.InputOutput => .{ .message = "failed to read configuration file", .detail = @errorName(err) },
        else => {
            const name = @errorName(err);
            if (std.mem.startsWith(u8, name, "AckError.")) {
                return RuntimeErrorDescription{ .message = "acknowledgement failed", .detail = name };
            }
            if (std.mem.startsWith(u8, name, "WriteError.")) {
                return RuntimeErrorDescription{ .message = "sink: write failed", .detail = name };
            }
            return RuntimeErrorDescription{ .message = "unexpected runtime failure", .detail = name };
        },
    };
}

fn describeParseError(err: cli.ParseError) []const u8 {
    return switch (err) {
        cli.ParseError.UnknownCommand => "unknown command",
        cli.ParseError.MissingCommand => "missing command",
        cli.ParseError.MissingOptionValue => "missing value for option",
        cli.ParseError.MissingConfigPath => "--config flag is required",
        cli.ParseError.UnexpectedArgument => "unexpected argument",
        cli.ParseError.InvalidNumber => "invalid numeric value",
    };
}

test "describe parse error" {
    try std.testing.expectEqualStrings("unknown command", describeParseError(cli.ParseError.UnknownCommand));
}

test "describe runtime error includes feature gate detail" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try sql_parser.parseSelect(arena, "SELECT DISTINCT message FROM logs");
    try std.testing.expectError(sql_runtime.Error.UnsupportedFeature, sql_runtime.compile(std.testing.allocator, stmt, .{}));

    const desc = describeRuntimeError(sql_runtime.Error.UnsupportedFeature);
    try std.testing.expectEqualStrings("sql runtime: unsupported feature", desc.message);
    const detail = desc.detail orelse return std.testing.expect(false);
    try std.testing.expect(std.mem.containsAtLeast(u8, detail, 1, "DISTINCT"));
    try std.testing.expect(std.mem.containsAtLeast(u8, detail, 1, "span"));
}

test "describe runtime error maps collector" {
    const desc = describeRuntimeError(collector_mod.CollectorError.UnsupportedSource);
    try std.testing.expectEqualStrings("collector: unsupported source type", desc.message);
    try std.testing.expect(desc.detail == null);
}

test "describe runtime error maps invalid limit" {
    const desc = describeRuntimeError(config_mod.ValidationError.InvalidLimit);
    try std.testing.expectEqualStrings("pipeline limit configuration is invalid", desc.message);
    try std.testing.expect(desc.detail == null);
}

test "describe runtime error exposes read detail" {
    const desc = describeRuntimeError(fs.File.ReadError.Unexpected);
    try std.testing.expectEqualStrings("failed to read configuration file", desc.message);
    try std.testing.expect(desc.detail != null);
}
