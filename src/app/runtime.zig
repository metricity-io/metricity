const std = @import("std");
const cli = @import("cli.zig");
const metricity = @import("metricity");
const config_parser = metricity.config_parser;
const config_mod = metricity.config;
const pipeline_mod = metricity.pipeline;
const source_mod = metricity.source;
const source_cfg = source_mod.config;
const builtin = @import("builtin");
const posix = std.posix;
const os = std.os;
const atomic_order = std.builtin.AtomicOrder;

var interrupt_requested = std.atomic.Value(bool).init(false);

/// All errors that `run`/`check` can surface to the CLI.
pub const Error = std.mem.Allocator.Error || config_parser.ParseError || source_cfg.ParseError || config_mod.ValidationError || pipeline_mod.Error || std.fs.File.OpenError || std.fs.File.ReadError || std.io.Writer.Error;

/// IO handles and allocator shared across runtime operations.
pub const Environment = struct {
    allocator: std.mem.Allocator,
    stdout: *std.io.Writer,
    stderr: *std.io.Writer,
};

pub fn run(env: Environment, options: cli.RunOptions) Error!void {
    var owned = try config_parser.parseFile(env.allocator, options.config_path);
    defer owned.deinit(env.allocator);

    try owned.pipeline.validate(env.allocator);

    var pipeline = try pipeline_mod.Pipeline.init(env.allocator, &owned.pipeline, .{});
    defer pipeline.deinit();
    defer pipeline.shutdown() catch {};

    interrupt_requested.store(false, atomic_order.seq_cst);
    installSignalHandlers();

    try pipeline.start();

    const target_batches = computeTargetBatches(options);
    var processed_batches: usize = 0;
    var interrupted = false;

    var idle_backoff_ns: u64 = min_idle_wait_ns;

    while (true) {
        if (target_batches) |limit| {
            if (processed_batches >= limit) break;
        }

        if (interrupt_requested.load(atomic_order.seq_cst)) {
            interrupted = true;
            break;
        }

        const processed = try pipeline.pollOnce();
        if (processed) {
            processed_batches += 1;
            idle_backoff_ns = min_idle_wait_ns;
            continue;
        }

        std.Thread.sleep(idle_backoff_ns);
        if (idle_backoff_ns < max_idle_wait_ns) {
            const doubled = idle_backoff_ns * 2;
            idle_backoff_ns = if (doubled > max_idle_wait_ns) max_idle_wait_ns else doubled;
        }
    }

    try pipeline.shutdown();

    if (interrupted) {
        try env.stderr.print("received termination signal, shutting down...\n", .{});
    }
}

pub fn check(env: Environment, options: cli.CheckOptions) Error!void {
    var owned = try config_parser.parseFile(env.allocator, options.config_path);
    defer owned.deinit(env.allocator);

    try owned.pipeline.validate(env.allocator);

    var pipeline = try pipeline_mod.Pipeline.init(env.allocator, &owned.pipeline, .{});
    defer pipeline.deinit();

    try env.stdout.print("Configuration OK\n", .{});
}

const min_idle_wait_ns: u64 = 50 * std.time.ns_per_us;
const max_idle_wait_ns: u64 = 50 * std.time.ns_per_ms;

fn computeTargetBatches(options: cli.RunOptions) ?usize {
    if (options.once) return 1;
    return options.max_batches;
}

fn installSignalHandlers() void {
    if (builtin.target.os.tag == .windows) {
        return;
    } else {
        var action = posix.Sigaction{
            .handler = .{ .handler = handleSignal },
            .mask = posix.sigemptyset(),
            .flags = 0,
        };
        if (@hasDecl(posix.SA, "RESTART")) {
            action.flags |= @as(@TypeOf(action.flags), posix.SA.RESTART);
        }
        posix.sigaction(posix.SIG.INT, &action, null);
        posix.sigaction(posix.SIG.TERM, &action, null);
    }
}

fn handleSignal(_: c_int) callconv(.c) void {
    interrupt_requested.store(true, atomic_order.seq_cst);
}
