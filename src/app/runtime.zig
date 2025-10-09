const std = @import("std");
const cli = @import("cli.zig");
const metricity = @import("metricity");
const config_parser = metricity.config_parser;
const config_mod = metricity.config;
const pipeline_mod = metricity.pipeline;
const source_mod = metricity.source;
const source_cfg = source_mod.config;
const collector_mod = metricity.collector;
const di = metricity.di;
const builtin = @import("builtin");
const posix = std.posix;
const os = std.os;
const atomic_order = std.builtin.AtomicOrder;

var interrupt_requested = std.atomic.Value(bool).init(false);

fn resetInterruptFlag() void {
    interrupt_requested.store(false, atomic_order.seq_cst);
}

const LoopHooks = struct {
    context: ?*anyopaque = null,
    sleep: *const fn (?*anyopaque, u64) void = defaultSleep,
};

const LoopResult = struct {
    processed_batches: usize,
    interrupted: bool,
};

/// All errors that `run`/`check` can surface to the CLI.
pub const Error = std.mem.Allocator.Error || config_parser.ParseError || source_cfg.ParseError || config_mod.ValidationError || pipeline_mod.Error || std.fs.File.OpenError || std.fs.File.ReadError || std.io.Writer.Error;

/// IO handles and allocator shared across runtime operations.
pub const Environment = struct {
    allocator: std.mem.Allocator,
   stdout: *std.io.Writer,
   stderr: *std.io.Writer,
};

const RuntimeServices = struct {
    parseFile: *const fn (std.mem.Allocator, []const u8) Error!config_mod.OwnedPipelineConfig,
    validate: *const fn (*const config_mod.PipelineConfig, std.mem.Allocator) (config_mod.ValidationError || std.mem.Allocator.Error)!void,
    pipelineFactory: *const fn (std.mem.Allocator, *const config_mod.PipelineConfig) pipeline_mod.Error!PipelineHandle,
    reportShutdown: *const fn (*std.io.Writer, LoopResult) std.io.Writer.Error!void,
    installSignalHandlers: *const fn () void,
};

const RuntimeContainer = di.Container(RuntimeServices);

fn defaultParseFile(allocator: std.mem.Allocator, path: []const u8) Error!config_mod.OwnedPipelineConfig {
    return config_parser.parseFile(allocator, path);
}

fn defaultValidate(cfg: *const config_mod.PipelineConfig, allocator: std.mem.Allocator) (config_mod.ValidationError || std.mem.Allocator.Error)!void {
    try cfg.validate(allocator);
}

fn defaultPipelineFactory(allocator: std.mem.Allocator, cfg: *const config_mod.PipelineConfig) pipeline_mod.Error!PipelineHandle {
    return createRealPipeline(allocator, cfg);
}

fn defaultRuntimeServices() RuntimeServices {
    return RuntimeServices{
        .parseFile = defaultParseFile,
        .validate = defaultValidate,
        .pipelineFactory = defaultPipelineFactory,
        .reportShutdown = reportShutdown,
        .installSignalHandlers = installSignalHandlers,
    };
}

pub const PipelineVTable = struct {
    start: *const fn (*anyopaque) pipeline_mod.Error!void,
    poll_once: *const fn (*anyopaque) pipeline_mod.Error!bool,
    shutdown: *const fn (*anyopaque) pipeline_mod.Error!void,
    deinit: *const fn (*anyopaque) void,
};

pub const PipelineHandle = struct {
    context: *anyopaque,
    vtable: *const PipelineVTable,

    fn start(self: *PipelineHandle) pipeline_mod.Error!void {
        return self.vtable.start(self.context);
    }

    fn pollOnce(self: *PipelineHandle) pipeline_mod.Error!bool {
        return self.vtable.poll_once(self.context);
    }

    fn shutdown(self: *PipelineHandle) pipeline_mod.Error!void {
        return self.vtable.shutdown(self.context);
    }

    fn deinit(self: *PipelineHandle) void {
        self.vtable.deinit(self.context);
    }
};

pub fn makePipelineHandle(context: *anyopaque, vtable: *const PipelineVTable) PipelineHandle {
    return PipelineHandle{
        .context = context,
        .vtable = vtable,
    };
}

const RealPipelineContext = struct {
    allocator: std.mem.Allocator,
    pipeline: pipeline_mod.Pipeline,
};

fn castRealPipeline(context: *anyopaque) *RealPipelineContext {
    return @ptrCast(@alignCast(context));
}

fn realPipelineStart(context: *anyopaque) pipeline_mod.Error!void {
    const ctx = castRealPipeline(context);
    try ctx.pipeline.start();
}

fn realPipelinePollOnce(context: *anyopaque) pipeline_mod.Error!bool {
    const ctx = castRealPipeline(context);
    return try ctx.pipeline.pollOnce();
}

fn realPipelineShutdown(context: *anyopaque) pipeline_mod.Error!void {
    const ctx = castRealPipeline(context);
    try ctx.pipeline.shutdown();
}

fn realPipelineDeinit(context: *anyopaque) void {
    const ctx = castRealPipeline(context);
    ctx.pipeline.deinit();
    ctx.allocator.destroy(ctx);
}

fn createRealPipeline(
    allocator: std.mem.Allocator,
    cfg: *const config_mod.PipelineConfig,
) pipeline_mod.Error!PipelineHandle {
    var pipeline_instance = try pipeline_mod.Pipeline.init(allocator, cfg, .{});
    const ctx = allocator.create(RealPipelineContext) catch |err| {
        pipeline_instance.deinit();
        return err;
    };
    ctx.* = .{
        .allocator = allocator,
        .pipeline = pipeline_instance,
    };
    return PipelineHandle{
        .context = ctx,
        .vtable = &real_pipeline_vtable,
    };
}

const real_pipeline_vtable = PipelineVTable{
    .start = realPipelineStart,
    .poll_once = realPipelinePollOnce,
    .shutdown = realPipelineShutdown,
    .deinit = realPipelineDeinit,
};

pub fn run(env: Environment, options: cli.RunOptions) Error!void {
    var container = RuntimeContainer.init(env.allocator, defaultRuntimeServices());
    defer container.deinit();
    return runWithContainer(env, options, &container);
}

pub fn runWithContainer(env: Environment, options: cli.RunOptions, container: *const RuntimeContainer) Error!void {
    const services = container.view();

    var owned = try services.parseFile(env.allocator, options.config_path);
    defer owned.deinit(env.allocator);

    try services.validate(&owned.pipeline, env.allocator);

    var pipeline = try services.pipelineFactory(env.allocator, &owned.pipeline);
    defer pipeline.deinit();

    resetInterruptFlag();
    services.installSignalHandlers();

    const target_batches = computeTargetBatches(options);
    const result = try executeRuntime(&pipeline, target_batches, LoopHooks{});
    try services.reportShutdown(env.stderr, result);
}

fn executeRuntime(pipeline: anytype, target_batches: ?usize, hooks: LoopHooks) Error!LoopResult {
    try pipeline.start();
    const result = try runLoop(pipeline, target_batches, hooks);
    try pipeline.shutdown();
    return result;
}

fn runLoop(pipeline: anytype, target_batches: ?usize, hooks: LoopHooks) Error!LoopResult {
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

        hooks.sleep(hooks.context, idle_backoff_ns);
        if (idle_backoff_ns < max_idle_wait_ns) {
            const doubled = idle_backoff_ns * 2;
            idle_backoff_ns = if (doubled > max_idle_wait_ns) max_idle_wait_ns else doubled;
        }
    }

    return LoopResult{
        .processed_batches = processed_batches,
        .interrupted = interrupted,
    };
}

fn defaultSleep(_: ?*anyopaque, ns: u64) void {
    std.Thread.sleep(ns);
}

fn reportShutdown(writer: *std.io.Writer, result: LoopResult) std.io.Writer.Error!void {
    if (result.interrupted) {
        try writer.print("received termination signal, shutting down...\n", .{});
    }
}

const runtime_testing = @import("../testing/app_runtime_di.zig").RuntimeTesting(
    RuntimeServices,
    PipelineHandle,
    PipelineVTable,
    pipeline_mod.Error,
    *const config_mod.PipelineConfig,
    makePipelineHandle,
);

const TestEnvironmentHarness = struct {
    stdout_file: std.fs.File = undefined,
    stderr_file: std.fs.File = undefined,
    stdout_buffer: [128]u8 = undefined,
    stderr_buffer: [128]u8 = undefined,
    stdout_writer: std.fs.File.Writer = undefined,
    stderr_writer: std.fs.File.Writer = undefined,
    env: Environment = undefined,

    fn init(self: *TestEnvironmentHarness, allocator: std.mem.Allocator) void {
        self.stdout_file = std.fs.File.stdout();
        self.stderr_file = std.fs.File.stderr();
        self.stdout_writer = self.stdout_file.writer(self.stdout_buffer[0..]);
        self.stderr_writer = self.stderr_file.writer(self.stderr_buffer[0..]);
        self.env = Environment{
            .allocator = allocator,
            .stdout = &self.stdout_writer.interface,
            .stderr = &self.stderr_writer.interface,
        };
    }
};

fn writeTempConfig(
    tmp: *std.testing.TmpDir,
    filename: []const u8,
    contents: []const u8,
) ![]u8 {
    try tmp.dir.writeFile(.{
        .sub_path = filename,
        .data = contents,
    });
    return try tmp.dir.realpathAlloc(std.testing.allocator, filename);
}

const TestSleepRecorder = struct {
    values: [16]u64 = [_]u64{0} ** 16,
    count: usize = 0,

    fn record(self: *TestSleepRecorder, value: u64) void {
        std.debug.assert(self.count < self.values.len);
        self.values[self.count] = value;
        self.count += 1;
    }

    fn items(self: *const TestSleepRecorder) []const u64 {
        return self.values[0..self.count];
    }
};

fn recorderFromContext(ctx: ?*anyopaque) *TestSleepRecorder {
    std.debug.assert(ctx != null);
    return @ptrCast(@alignCast(ctx.?));
}

fn recordSleep(ctx: ?*anyopaque, ns: u64) void {
    const recorder = recorderFromContext(ctx);
    recorder.record(ns);
}

const PipelineStub = struct {
    poll_sequence: []const bool = &.{},
    default_result: bool = false,
    interrupt_after: ?usize = null,
    start_calls: usize = 0,
    shutdown_calls: usize = 0,
    poll_calls: usize = 0,
    poll_index: usize = 0,
    interrupt_triggered: bool = false,

    fn start(self: *PipelineStub) Error!void {
        self.start_calls += 1;
    }

    fn pollOnce(self: *PipelineStub) Error!bool {
        self.poll_calls += 1;
        if (self.interrupt_after) |threshold| {
            if (!self.interrupt_triggered and self.poll_calls >= threshold) {
                interrupt_requested.store(true, atomic_order.seq_cst);
                self.interrupt_triggered = true;
            }
        }

        if (self.poll_index < self.poll_sequence.len) {
            const result = self.poll_sequence[self.poll_index];
            self.poll_index += 1;
            return result;
        }
        return self.default_result;
    }

    fn shutdown(self: *PipelineStub) Error!void {
        self.shutdown_calls += 1;
    }
};

pub fn check(env: Environment, options: cli.CheckOptions) Error!void {
    var container = RuntimeContainer.init(env.allocator, defaultRuntimeServices());
    defer container.deinit();
    return checkWithContainer(env, options, &container);
}

pub fn checkWithContainer(env: Environment, options: cli.CheckOptions, container: *const RuntimeContainer) Error!void {
    const services = container.view();

    var owned = try services.parseFile(env.allocator, options.config_path);
    defer owned.deinit(env.allocator);

    try services.validate(&owned.pipeline, env.allocator);

    var pipeline = try services.pipelineFactory(env.allocator, &owned.pipeline);
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

test "handleSignal sets interrupt flag" {
    resetInterruptFlag();
    try std.testing.expectEqual(false, interrupt_requested.load(atomic_order.seq_cst));
    handleSignal(0);
    try std.testing.expectEqual(true, interrupt_requested.load(atomic_order.seq_cst));
    resetInterruptFlag();
}

test "handleSignal is idempotent" {
    resetInterruptFlag();
    handleSignal(0);
    handleSignal(0);
    try std.testing.expectEqual(true, interrupt_requested.load(atomic_order.seq_cst));
    resetInterruptFlag();
}

test "resetInterruptFlag clears pending interrupts" {
    interrupt_requested.store(true, atomic_order.seq_cst);
    resetInterruptFlag();
    try std.testing.expectEqual(false, interrupt_requested.load(atomic_order.seq_cst));
}

test "computeTargetBatches once overrides max batches" {
    var options = cli.RunOptions{
        .config_path = "",
        .once = true,
        .max_batches = null,
    };
    try std.testing.expectEqual(@as(usize, 1), computeTargetBatches(options).?);

    options.max_batches = 0;
    try std.testing.expectEqual(@as(usize, 1), computeTargetBatches(options).?);

    options.max_batches = 42;
    try std.testing.expectEqual(@as(usize, 1), computeTargetBatches(options).?);
}

test "computeTargetBatches returns null in continuous mode" {
    const options = cli.RunOptions{
        .config_path = "",
        .once = false,
        .max_batches = null,
    };
    try std.testing.expect(computeTargetBatches(options) == null);
}

test "computeTargetBatches passes through positive limits" {
    const options = cli.RunOptions{
        .config_path = "",
        .once = false,
        .max_batches = 5,
    };
    try std.testing.expectEqual(@as(usize, 5), computeTargetBatches(options).?);
}

test "computeTargetBatches supports zero limit" {
    const options = cli.RunOptions{
        .config_path = "",
        .once = false,
        .max_batches = 0,
    };
    try std.testing.expectEqual(@as(usize, 0), computeTargetBatches(options).?);
}

test "computeTargetBatches is pure for identical inputs" {
    const options = cli.RunOptions{
        .config_path = "",
        .once = false,
        .max_batches = null,
    };

    const first = computeTargetBatches(options);
    const second = computeTargetBatches(options);
    try std.testing.expect(first == second);
}

test "executeRuntime exits immediately when max batches is zero" {
    resetInterruptFlag();
    defer resetInterruptFlag();

    var pipeline = PipelineStub{};
    var recorder = TestSleepRecorder{};
    const hooks = LoopHooks{ .context = &recorder, .sleep = recordSleep };
    const zero_limit: ?usize = 0;
    const result = try executeRuntime(&pipeline, zero_limit, hooks);

    try std.testing.expectEqual(@as(usize, 1), pipeline.start_calls);
    try std.testing.expectEqual(@as(usize, 1), pipeline.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 0), pipeline.poll_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.items().len);
    try std.testing.expectEqual(@as(usize, 0), result.processed_batches);
    try std.testing.expect(!result.interrupted);
}

test "executeRuntime limits processed batches" {
    resetInterruptFlag();
    defer resetInterruptFlag();

    var pipeline = PipelineStub{
        .poll_sequence = &.{ true, true, true },
    };
    var recorder = TestSleepRecorder{};
    const hooks = LoopHooks{ .context = &recorder, .sleep = recordSleep };
    const target: ?usize = 3;
    const result = try executeRuntime(&pipeline, target, hooks);

    try std.testing.expectEqual(@as(usize, 1), pipeline.start_calls);
    try std.testing.expectEqual(@as(usize, 1), pipeline.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 3), pipeline.poll_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.items().len);
    try std.testing.expectEqual(@as(usize, 3), result.processed_batches);
    try std.testing.expect(!result.interrupted);
}

test "runLoop resets idle backoff after work" {
    resetInterruptFlag();
    defer resetInterruptFlag();

    var pipeline = PipelineStub{
        .poll_sequence = &.{ false, false, true, false },
        .interrupt_after = 4,
    };
    var recorder = TestSleepRecorder{};
    const hooks = LoopHooks{ .context = &recorder, .sleep = recordSleep };
    const result = try executeRuntime(&pipeline, null, hooks);

    const sleeps = recorder.items();
    try std.testing.expectEqual(@as(usize, 3), sleeps.len);
    try std.testing.expectEqual(min_idle_wait_ns, sleeps[0]);
    try std.testing.expectEqual(min_idle_wait_ns * 2, sleeps[1]);
    try std.testing.expectEqual(min_idle_wait_ns, sleeps[2]);
    try std.testing.expectEqual(@as(usize, 1), result.processed_batches);
    try std.testing.expect(result.interrupted);
}

test "runLoop applies exponential idle backoff until capped" {
    resetInterruptFlag();
    defer resetInterruptFlag();

    const idle_sequence = [_]bool{
        false, false, false, false, false, false,
        false, false, false, false, false, false,
    };
    var pipeline = PipelineStub{
        .poll_sequence = idle_sequence[0..],
        .interrupt_after = idle_sequence.len,
    };
    var recorder = TestSleepRecorder{};
    const hooks = LoopHooks{ .context = &recorder, .sleep = recordSleep };
    const result = try executeRuntime(&pipeline, null, hooks);

    const expected = [_]u64{
        min_idle_wait_ns,
        min_idle_wait_ns * 2,
        min_idle_wait_ns * 4,
        min_idle_wait_ns * 8,
        min_idle_wait_ns * 16,
        min_idle_wait_ns * 32,
        min_idle_wait_ns * 64,
        min_idle_wait_ns * 128,
        min_idle_wait_ns * 256,
        min_idle_wait_ns * 512,
        max_idle_wait_ns,
        max_idle_wait_ns,
    };
    const sleeps = recorder.items();
    try std.testing.expectEqual(@as(usize, expected.len), sleeps.len);
    try std.testing.expectEqualSlices(u64, expected[0..], sleeps);

    try std.testing.expectEqual(@as(usize, idle_sequence.len), pipeline.poll_calls);
    try std.testing.expectEqual(@as(usize, 1), pipeline.start_calls);
    try std.testing.expectEqual(@as(usize, 1), pipeline.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 0), result.processed_batches);
    try std.testing.expect(result.interrupted);
}

test "runLoop saturates idle backoff at max without overshoot" {
    resetInterruptFlag();
    defer resetInterruptFlag();

    const idle_sequence = [_]bool{
        false, false, false, false, false, false,
        false, false, false, false, false,
    };
    var pipeline = PipelineStub{
        .poll_sequence = idle_sequence[0..],
        .interrupt_after = idle_sequence.len,
    };
    var recorder = TestSleepRecorder{};
    const hooks = LoopHooks{ .context = &recorder, .sleep = recordSleep };
    const result = try executeRuntime(&pipeline, null, hooks);

    const sleeps = recorder.items();
    try std.testing.expectEqual(@as(usize, idle_sequence.len), sleeps.len);

    const last_index = sleeps.len - 1;
    const penultimate_value = min_idle_wait_ns * 512;

    try std.testing.expectEqual(penultimate_value, sleeps[last_index - 1]);
    try std.testing.expectEqual(max_idle_wait_ns, sleeps[last_index]);
    try std.testing.expect(sleeps[last_index - 1] < max_idle_wait_ns);

    for (sleeps) |value| {
        try std.testing.expect(value <= max_idle_wait_ns);
    }

    try std.testing.expectEqual(@as(usize, idle_sequence.len), pipeline.poll_calls);
    try std.testing.expectEqual(@as(usize, 1), pipeline.start_calls);
    try std.testing.expectEqual(@as(usize, 1), pipeline.shutdown_calls);
    try std.testing.expect(result.interrupted);
}

test "executeRuntime stops after interrupt request" {
    resetInterruptFlag();
    defer resetInterruptFlag();

    var pipeline = PipelineStub{
        .poll_sequence = &.{ false, false, false },
        .interrupt_after = 3,
    };
    var recorder = TestSleepRecorder{};
    const hooks = LoopHooks{ .context = &recorder, .sleep = recordSleep };
    const result = try executeRuntime(&pipeline, null, hooks);

    try std.testing.expect(result.interrupted);
    try std.testing.expectEqual(@as(usize, 1), pipeline.start_calls);
    try std.testing.expectEqual(@as(usize, 1), pipeline.shutdown_calls);

    var stderr_buffer: [128]u8 = undefined;
    var stderr_writer = std.io.Writer.fixed(stderr_buffer[0..]);
    try reportShutdown(&stderr_writer, result);
    const written = std.io.Writer.buffered(&stderr_writer);
    try std.testing.expectEqualStrings("received termination signal, shutting down...\n", written);
}

test "reportShutdown stays silent without interrupt" {
    resetInterruptFlag();
    defer resetInterruptFlag();

    var pipeline = PipelineStub{
        .poll_sequence = &.{ true, true },
    };
    var recorder = TestSleepRecorder{};
    const hooks = LoopHooks{ .context = &recorder, .sleep = recordSleep };
    const limit: ?usize = 2;
    const result = try executeRuntime(&pipeline, limit, hooks);
    try std.testing.expect(!result.interrupted);

    var stderr_buffer: [64]u8 = undefined;
    var stderr_writer = std.io.Writer.fixed(stderr_buffer[0..]);
    try reportShutdown(&stderr_writer, result);
    const written = std.io.Writer.buffered(&stderr_writer);
    try std.testing.expectEqual(@as(usize, 0), written.len);
}

test "runWithContainer invokes installSignalHandlers service" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_path = try writeTempConfig(&tmp, "signals.toml", valid_runtime_config);
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    const options = cli.RunOptions{
            .config_path = config_path,
            .max_batches = null,
            .once = true,
    };

    var double = runtime_testing.PipelineDouble{
            .poll_steps = &.{ .{ .value = true } },
            .default_poll_result = false,
    };

    const base_services = defaultRuntimeServices();
    const services_with_double = runtime_testing.servicesWithPipelineDouble(base_services, &double);
    defer runtime_testing.clearOverride();

    const Hook = struct {
            var calls: usize = 0;
            fn install() void {
                calls += 1;
            }
    };
    Hook.calls = 0;

    var container = RuntimeContainer.init(harness.env.allocator, base_services)
        .withPartial(@TypeOf(.{
            .pipelineFactory = services_with_double.pipelineFactory,
            .installSignalHandlers = Hook.install,
        }), .{
            .pipelineFactory = services_with_double.pipelineFactory,
            .installSignalHandlers = Hook.install,
        });
    defer container.deinit();

    try runWithContainer(harness.env, options, &container);
    try std.testing.expectEqual(@as(usize, 1), Hook.calls);
}

const valid_runtime_config =
    \\[sources.input]
    \\type = "syslog"
    \\address = "udp://127.0.0.1:5514"
    \\outputs = ["sink"]
    \\
    \\[sinks.sink]
    \\type = "console"
    \\inputs = ["input"]
    \\target = "stdout"
;

test "run surfaces config parse errors" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_path = try writeTempConfig(&tmp, "invalid.toml", "[sources.invalid");
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    const options = cli.RunOptions{
        .config_path = config_path,
        .max_batches = null,
        .once = false,
    };

    try std.testing.expectError(config_parser.ParseError.InvalidSyntax, run(harness.env, options));
}

test "run surfaces validation errors" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_text =
        \\[sources.input]
        \\type = "syslog"
        \\address = "udp://127.0.0.1:5514"
        \\outputs = []
        \\
    ;

    const config_path = try writeTempConfig(&tmp, "missing_sinks.toml", config_text);
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    const options = cli.RunOptions{
        .config_path = config_path,
        .max_batches = null,
        .once = false,
    };

    try std.testing.expectError(config_mod.ValidationError.MissingSinks, run(harness.env, options));
}

test "run surfaces pipeline init errors" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_path = try writeTempConfig(&tmp, "pipeline_init.toml", valid_runtime_config);
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    const options = cli.RunOptions{
        .config_path = config_path,
        .max_batches = null,
        .once = false,
    };
    const init_override = struct {
        fn factory(_: std.mem.Allocator, _: *const config_mod.PipelineConfig) pipeline_mod.Error!PipelineHandle {
            return error.CycleDetected;
        }
    }.factory;

    var container = RuntimeContainer.init(harness.env.allocator, defaultRuntimeServices())
        .withPartial(@TypeOf(.{ .pipelineFactory = init_override }), .{
        .pipelineFactory = init_override,
    });
    defer container.deinit();

    try std.testing.expectError(
        pipeline_mod.Error.CycleDetected,
        runWithContainer(harness.env, options, &container),
    );
}

test "run surfaces pipeline start errors and deinit remains safe" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_path = try writeTempConfig(&tmp, "start_error.toml", valid_runtime_config);
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    var double = runtime_testing.PipelineDouble{
        .start_error = collector_mod.CollectorError.StartFailed,
    };

    const options = cli.RunOptions{
        .config_path = config_path,
        .max_batches = null,
        .once = false,
    };
    const base_services = defaultRuntimeServices();
    const services_with_double = runtime_testing.servicesWithPipelineDouble(base_services, &double);
    defer runtime_testing.clearOverride();

    var container = RuntimeContainer.init(harness.env.allocator, base_services)
        .withPartial(@TypeOf(.{ .pipelineFactory = services_with_double.pipelineFactory }), .{
        .pipelineFactory = services_with_double.pipelineFactory,
    });
    defer container.deinit();

    try std.testing.expectError(
        collector_mod.CollectorError.StartFailed,
        runWithContainer(harness.env, options, &container),
    );
    try std.testing.expectEqual(@as(usize, 1), double.start_calls);
    try std.testing.expectEqual(@as(usize, 0), double.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), double.deinit_calls);
}

test "run surfaces pollOnce errors and shuts down pipeline" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_path = try writeTempConfig(&tmp, "poll_error.toml", valid_runtime_config);
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    const poll_steps = [_]runtime_testing.PipelineDouble.PollStep{
        .{ .err = collector_mod.CollectorError.PollFailed },
    };

    var double = runtime_testing.PipelineDouble{
        .poll_steps = poll_steps[0..],
        .default_poll_result = false,
    };

    const options = cli.RunOptions{
        .config_path = config_path,
        .max_batches = null,
        .once = false,
    };
    const base_services = defaultRuntimeServices();
    const services_with_double = runtime_testing.servicesWithPipelineDouble(base_services, &double);
    defer runtime_testing.clearOverride();

    var container = RuntimeContainer.init(harness.env.allocator, base_services)
        .withPartial(@TypeOf(.{ .pipelineFactory = services_with_double.pipelineFactory }), .{
        .pipelineFactory = services_with_double.pipelineFactory,
    });
    defer container.deinit();

    try std.testing.expectError(
        collector_mod.CollectorError.PollFailed,
        runWithContainer(harness.env, options, &container),
    );
    try std.testing.expectEqual(@as(usize, 1), double.start_calls);
    try std.testing.expectEqual(@as(usize, 1), double.poll_calls);
    try std.testing.expectEqual(@as(usize, 1), double.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), double.deinit_calls);
}

test "run surfaces shutdown errors" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_path = try writeTempConfig(&tmp, "shutdown_error.toml", valid_runtime_config);
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    const poll_steps = [_]runtime_testing.PipelineDouble.PollStep{
        .{ .value = true },
    };

    var double = runtime_testing.PipelineDouble{
        .poll_steps = poll_steps[0..],
        .default_poll_result = false,
        .shutdown_error = collector_mod.CollectorError.ShutdownFailed,
    };

    const options = cli.RunOptions{
        .config_path = config_path,
        .max_batches = null,
        .once = true,
    };
    const base_services = defaultRuntimeServices();
    const services_with_double = runtime_testing.servicesWithPipelineDouble(base_services, &double);
    defer runtime_testing.clearOverride();

    var container = RuntimeContainer.init(harness.env.allocator, base_services)
        .withPartial(@TypeOf(.{ .pipelineFactory = services_with_double.pipelineFactory }), .{
        .pipelineFactory = services_with_double.pipelineFactory,
    });
    defer container.deinit();

    try std.testing.expectError(
        collector_mod.CollectorError.ShutdownFailed,
        runWithContainer(harness.env, options, &container),
    );
    try std.testing.expectEqual(@as(usize, 1), double.start_calls);
    try std.testing.expectEqual(@as(usize, 1), double.poll_calls);
    try std.testing.expectEqual(@as(usize, 1), double.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), double.deinit_calls);
}

test "run stops pipeline exactly once" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_path = try writeTempConfig(&tmp, "idempotent_shutdown.toml", valid_runtime_config);
    defer std.testing.allocator.free(config_path);

    var harness = TestEnvironmentHarness{};
    harness.init(std.testing.allocator);

    const poll_steps = [_]runtime_testing.PipelineDouble.PollStep{
        .{ .value = true },
    };

    var double = runtime_testing.PipelineDouble{
        .poll_steps = poll_steps[0..],
        .default_poll_result = false,
    };

    const options = cli.RunOptions{
        .config_path = config_path,
        .max_batches = null,
        .once = true,
    };
    const base_services = defaultRuntimeServices();
    const services_with_double = runtime_testing.servicesWithPipelineDouble(base_services, &double);
    defer runtime_testing.clearOverride();

    var container = RuntimeContainer.init(harness.env.allocator, base_services)
        .withPartial(@TypeOf(.{ .pipelineFactory = services_with_double.pipelineFactory }), .{
        .pipelineFactory = services_with_double.pipelineFactory,
    });
    defer container.deinit();

    try runWithContainer(harness.env, options, &container);
    try std.testing.expectEqual(@as(usize, 1), double.start_calls);
    try std.testing.expectEqual(@as(usize, 1), double.poll_calls);
    try std.testing.expectEqual(@as(usize, 1), double.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), double.deinit_calls);
}
