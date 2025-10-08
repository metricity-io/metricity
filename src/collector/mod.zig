const std = @import("std");
const source = @import("source");
const netx = @import("netx");
const cfg = source.config;
const event = source.event;
const meta = std.meta;
const math = std.math;
const restart = @import("restart");
const ReadyQueue = std.ArrayListUnmanaged(usize);

/// Normalised error surface exposed by the collector during lifecycle and polling
/// operations.
pub const CollectorError = error{
    InvalidConfiguration,
    InitializationFailed,
    StartFailed,
    PollFailed,
    NotStarted,
    Backpressure,
    ShutdownFailed,
    UnsupportedSource,
    Unimplemented,
};

const Clock = struct {
    context: ?*anyopaque = null,
    now_fn: *const fn (context: ?*anyopaque) u128,

    pub fn monotonic() Clock {
        return .{ .context = null, .now_fn = defaultNow };
    }

    pub fn now(self: Clock) u128 {
        return self.now_fn(self.context);
    }

    fn defaultNow(_: ?*anyopaque) u128 {
        const value = std.time.nanoTimestamp();
        if (value <= 0) return 0;
        return @as(u128, @intCast(value));
    }
};

pub const RestartOptions = struct {
    clock: ?Clock = null,
    rng_seed: ?u64 = null,
};

/// Customisation hooks allowing embedders to provide alternative source registry,
/// logging and metrics plumbing.
pub const CollectorOptions = struct {
    registry: source.Registry = source.Registry.builtin(),
    log: ?*const source.Logger = null,
    metrics: ?*const source.Metrics = null,
    restart: RestartOptions = .{},
};

/// Metric identifiers emitted by the collector when metrics plumbing is enabled.
const collector_metrics = struct {
    const batches = "collector_batches_emitted_total";
    const events = "collector_events_emitted_total";
    const poll_errors = "collector_poll_errors_total";
    const backpressure = "collector_backpressure_total";
    const active_sources = "collector_active_sources";
    const failed_sources = "collector_failed_sources";
    const ready_skips = "collector_ready_skips_total";
    const empty_polls = "collector_empty_polls_total";
    const restart_failures = "collector_restart_failures_total";
    const restart_quarantine = "collector_restart_quarantine";
};

const max_ready_skip_cycles: usize = 8;
const max_ready_queue_depth: usize = 16;
const max_productive_streak: usize = 3;
const max_stream_error_streak: u8 = 3;

const FailurePermanence = enum { transient, permanent };

const FailureSeverity = enum { warn, err };

const FailureKind = enum {
    start_invalid_configuration,
    start_unsupported,
    start_not_implemented,
    start_startup_failed,
    start_backpressure,
    start_unknown,
    poll_transport_failure,
    poll_decode_failure,
    poll_invalid_configuration,
    poll_backpressure,
    poll_unknown,
};

const LifecycleFailure = struct {
    collector_error: CollectorError,
    permanence: FailurePermanence,
    restartable: bool,
    severity: FailureSeverity,
    kind: FailureKind,
};

const RestartPolicy = restart.RestartPolicy;
const RestartState = restart.RestartState(FailureKind, FailurePermanence);

const syslog_restart_policy = RestartPolicy{
    .backoff = .{
        .min_ns = 500 * std.time.ns_per_ms,
        .max_ns = 30 * std.time.ns_per_s,
        .multiplier_num = 18,
        .multiplier_den = 10,
        .jitter = .full,
    },
    .budget = .{
        .window_ns = 60 * std.time.ns_per_s,
        .limit = 8,
        .hold_ns = 60 * std.time.ns_per_s,
    },
};

/// Structured logging helper used for informational messages.
fn logInfo(options: CollectorOptions, comptime fmt: []const u8, args: anytype) void {
    if (options.log) |logger| {
        logger.infof(fmt, args);
    }
}

/// Structured logging helper used for warnings (typically signalling transient
/// backpressure).
fn logWarn(options: CollectorOptions, comptime fmt: []const u8, args: anytype) void {
    if (options.log) |logger| {
        logger.warnf(fmt, args);
    }
}

/// Structured logging helper used for permanent failures.
fn logError(options: CollectorOptions, comptime fmt: []const u8, args: anytype) void {
    if (options.log) |logger| {
        logger.errorf(fmt, args);
    }
}

fn logFailure(
    options: CollectorOptions,
    severity: FailureSeverity,
    permanence: FailurePermanence,
    comptime fmt: []const u8,
    args: anytype,
) void {
    if (options.log) |logger| {
        const message = "[permanence={s}] " ++ fmt;
        const prefixed_args = .{@tagName(permanence)} ++ args;
        switch (severity) {
            .warn => logger.warnf(message, prefixed_args),
            .err => logger.errorf(message, prefixed_args),
        }
    }
}

/// Convenience helper for incrementing numeric metrics.
fn recordCounter(options: CollectorOptions, name: []const u8, value: u64) void {
    if (options.metrics) |metrics_sink| {
        metrics_sink.incrCounter(name, value);
    }
}

const ActiveTransport = union(enum) {
    none,
    stream: event.EventStream,
    batch,
};

const ReadySignalContext = struct {
    collector: *Collector,
    index: usize,

    fn notify(context: *anyopaque) void {
        const aligned: *align(@alignOf(ReadySignalContext)) anyopaque = @alignCast(context);
        const self: *ReadySignalContext = @ptrCast(aligned);
        self.collector.enqueueReady(self.index);
    }
};

/// Carrier struct used internally to track the lifecycle of each configured source.
pub const SourceHandle = struct {
    descriptor: source.SourceDescriptor,
    instance: source.Source,
    transport: ActiveTransport = .none,
    supports_batching: bool,
    shutting_down: bool = false,
    ready_skip_count: usize = 0,
    consecutive_empty_polls: usize = 0,
    consecutive_stream_errors: u8 = 0,
    stream_error_window_count: u8 = 0,
    stream_error_window_start_ns: u128 = 0,
    ready_signal_depth: usize = 0,
    productive_streak: usize = 0,
    ready_signal_context: ?*ReadySignalContext = null,
    ready_signal_registered: bool = false,
    restart_policy: RestartPolicy,
    restart_state: RestartState = .{},
    poll_error_counter_name: []const u8 = "",
};

fn activateStreamTransport(handle: *SourceHandle, stream: event.EventStream) void {
    handle.transport = .{ .stream = stream };
}

fn activateBatchTransport(handle: *SourceHandle) void {
    handle.transport = .batch;
}

fn resetTransport(handle: *SourceHandle, allocator: std.mem.Allocator) void {
    switch (handle.transport) {
        .stream => |*stream_handle| stream_handle.finish(allocator),
        else => {},
    }
    handle.transport = .none;
}

fn resetStreamErrorTracking(handle: *SourceHandle) void {
    handle.consecutive_stream_errors = 0;
    handle.stream_error_window_count = 0;
    handle.stream_error_window_start_ns = 0;
}

fn registerStreamError(handle: *SourceHandle, now: u128) bool {
    if (handle.consecutive_stream_errors < math.maxInt(u8)) {
        handle.consecutive_stream_errors += 1;
    }

    const budget = handle.restart_policy.budget;
    var window_trigger = false;

    if (budget.limit != 0) {
        if (handle.stream_error_window_count == 0) {
            handle.stream_error_window_start_ns = now;
            handle.stream_error_window_count = 1;
        } else {
            const window_ns = @as(u128, budget.window_ns);
            if (window_ns == 0) {
                if (handle.stream_error_window_count < math.maxInt(u8)) {
                    handle.stream_error_window_count += 1;
                }
            } else {
                const start = handle.stream_error_window_start_ns;
                const delta = if (now >= start) now - start else start - now;
                if (delta > window_ns) {
                    handle.stream_error_window_start_ns = now;
                    handle.stream_error_window_count = 1;
                } else if (handle.stream_error_window_count < math.maxInt(u8)) {
                    handle.stream_error_window_count += 1;
                }
            }
        }

        window_trigger = handle.stream_error_window_count >= budget.limit;
    }

    const streak_trigger = handle.consecutive_stream_errors >= max_stream_error_streak;
    return streak_trigger or window_trigger;
}

fn transportIsStreaming(handle: *const SourceHandle) bool {
    return switch (handle.transport) {
        .stream => true,
        else => false,
    };
}

const PollStatus = enum {
    idle,
    yielded,
    empty,
    finished,
    backpressure,
};

const PollResult = struct {
    status: PollStatus,
    batch: ?event.EventBatch = null,
    attempted: bool,
};

/// Batch returned by the collector, pairing the originating descriptor with events
/// and acknowledgement token.
pub const PolledBatch = struct {
    descriptor: source.SourceDescriptor,
    batch: event.EventBatch,
};

/// Snapshot of collector progress, used by diagnostics and metrics.
pub const Status = struct {
    total_sources: usize,
    active_sources: usize,
    streaming_sources: usize,
    started: bool,
};

/// Multi-source orchestrator that handles stream start-up, batch polling and
/// fan-out to downstream pipeline components.
pub const Collector = struct {
    allocator: std.mem.Allocator,
    runtime: *netx.runtime.IoRuntime,
    options: CollectorOptions,
    clock: Clock,
    prng: std.Random.DefaultPrng,
    sources: std.ArrayListUnmanaged(SourceHandle) = .{},
    next_index: usize = 0,
    started: bool = false,
    ready_queue: ReadyQueue = .{},
    ready_queue_head: usize = 0,

    const PollIndexStatus = enum {
        skipped,
        idle,
        yielded,
        backpressure,
    };

    const PollIndexResult = struct {
        status: PollIndexStatus,
        next_index: usize,
        batch: ?PolledBatch = null,
    };

    /// Instantiates source handles and brings up the IO runtime. Sources are
    /// created but not yet streaming or polling.
    pub fn init(
        allocator: std.mem.Allocator,
        configs: []const cfg.SourceConfig,
        options: CollectorOptions,
    ) CollectorError!Collector {
        var collector = Collector{
            .allocator = allocator,
            .runtime = undefined,
            .options = options,
            .clock = options.restart.clock orelse Clock.monotonic(),
            .prng = undefined,
            .sources = .{},
            .next_index = 0,
            .started = false,
            .ready_queue = .{},
            .ready_queue_head = 0,
        };

        const seed_salt = @intFromPtr(&collector);
        const seed = options.restart.rng_seed orelse deriveSeed(collector.clock, seed_salt);
        collector.prng = std.Random.DefaultPrng.init(seed);

        const runtime_ptr = allocator.create(netx.runtime.IoRuntime) catch {
            return CollectorError.InitializationFailed;
        };
        errdefer allocator.destroy(runtime_ptr);
        runtime_ptr.* = netx.runtime.IoRuntime.initDefault() catch {
            allocator.destroy(runtime_ptr);
            return CollectorError.InitializationFailed;
        };
        collector.runtime = runtime_ptr;

        var cleanup_needed = true;
        errdefer if (cleanup_needed) {
            destroySourceHandles(&collector, allocator);
            collector.sources.deinit(allocator);
            collector.runtime.deinit();
            allocator.destroy(collector.runtime);
        };

        for (configs) |config| {
            const source_type = meta.activeTag(config.payload);
            const factory = options.registry.findFactory(source_type) orelse {
                return CollectorError.UnsupportedSource;
            };

            const init_ctx = source.InitContext{
                .allocator = allocator,
                .runtime = collector.runtime,
                .log = options.log,
                .metrics = options.metrics,
            };

            var instance = factory.create(init_ctx, &config) catch {
                return CollectorError.InitializationFailed;
            };

            const descriptor = instance.descriptor();
            const handle = SourceHandle{
                .descriptor = descriptor,
                .instance = instance,
                .transport = .none,
                .supports_batching = instance.supportsBatching(),
                .shutting_down = false,
                .restart_policy = selectRestartPolicy(descriptor),
                .restart_state = RestartState.init(.{
                    .context = &collector,
                    .next_fn = restartRandomFromCollector,
                }),
            };

            collector.sources.append(allocator, handle) catch {
                instance.shutdown(allocator);
                return CollectorError.InitializationFailed;
            };
        }

        cleanup_needed = false;
        return collector;
    }

    /// Tears down all sources and releases the backing IO runtime.
    pub fn deinit(self: *Collector) void {
        self.clearReadyQueue();
        destroySourceHandles(self, self.allocator);
        self.ready_queue.deinit(self.allocator);
        self.sources.deinit(self.allocator);
        self.runtime.deinit();
        self.allocator.destroy(self.runtime);
    }

    /// Starts streaming and/or batching for each configured source. Idempotent.
    pub fn start(self: *Collector, allocator: std.mem.Allocator) CollectorError!void {
        if (self.started) return;

        var fatal_error: ?CollectorError = null;

        for (self.sources.items, 0..) |*handle, index| {
            if (handle.shutting_down) {
                logError(self.options, "collector start aborted: source {s} already shutdown", .{handle.descriptor.name});
                if (fatal_error == null) fatal_error = CollectorError.StartFailed;
                continue;
            }

            const capabilities = handle.instance.capabilities();
            if (!capabilities.streaming and !handle.supports_batching) {
                logError(
                    self.options,
                    "collector start aborted: source {s} exposes neither streaming nor batching",
                    .{handle.descriptor.name},
                );
                handle.shutting_down = true;
                if (fatal_error == null) fatal_error = CollectorError.InvalidConfiguration;
                continue;
            }

            resetTransport(handle, allocator);
            if (capabilities.streaming) {
                var start_failed = false;
                const maybe_stream = handle.instance.startStream(allocator) catch |err| blk: {
                    start_failed = true;
                    const failure = classifyStartError(err);
                    if (failure.kind == .start_backpressure) {
                        recordBackpressure(self);
                    }
                    logFailure(
                        self.options,
                        failure.severity,
                        failure.permanence,
                        "collector start failure for {s}: {s}",
                        .{ handle.descriptor.name, @errorName(err) },
                    );

                    const now = self.clock.now();
                    if (failure.restartable and failure.permanence == .transient) {
                        self.handleTransientFailure(handle, failure, now);
                    } else {
                        self.handlePermanentFailure(handle, failure);
                    }

                    if (failure.permanence == .permanent or failure.collector_error == CollectorError.Backpressure) {
                        if (fatal_error == null) fatal_error = failure.collector_error;
                    }

                    break :blk null;
                };

                if (start_failed) continue;

                if (maybe_stream) |stream| {
                    activateStreamTransport(handle, stream);
                    restart.recordSuccess(&handle.restart_state);
                    logInfo(self.options, "collector started stream for {s}", .{handle.descriptor.name});
                } else if (!handle.supports_batching) {
                    logError(
                        self.options,
                        "collector start aborted: source {s} did not expose stream",
                        .{handle.descriptor.name},
                    );
                    if (fatal_error == null) fatal_error = CollectorError.StartFailed;
                } else {
                    restart.recordSuccess(&handle.restart_state);
                    activateBatchTransport(handle);
                    logInfo(
                        self.options,
                        "collector using batch polling for {s}",
                        .{handle.descriptor.name},
                    );
                }
            } else if (handle.supports_batching) {
                restart.recordSuccess(&handle.restart_state);
                activateBatchTransport(handle);
                logInfo(self.options, "collector using batch-only mode for {s}", .{handle.descriptor.name});
            }

            if (!handle.shutting_down) {
                self.registerReadyObserver(handle, index);
            }
        }

        self.refreshQuarantineGauge();
        updateActiveSourcesGauge(self);
        if (fatal_error) |err| {
            return err;
        }

        self.started = true;
        self.next_index = 0;
        logInfo(self.options, "collector started ({d} sources)", .{self.sources.items.len});
    }

    /// Returns a snapshot that can be emitted via logs/metrics to describe the
    /// collector state.
    pub fn status(self: *const Collector) Status {
        var active: usize = 0;
        var streaming: usize = 0;

        for (self.sources.items) |*handle| {
            if (!handle.shutting_down) active += 1;
            if (transportIsStreaming(handle)) streaming += 1;
        }

        return Status{
            .total_sources = self.sources.items.len,
            .active_sources = active,
            .streaming_sources = streaming,
            .started = self.started,
        };
    }

    fn nextRandom(self: *Collector) u64 {
        return self.prng.random().int(u64);
    }

fn restartRandomFromCollector(context: ?*anyopaque) u64 {
    std.debug.assert(context != null);
    const aligned: *align(@alignOf(Collector)) anyopaque = @alignCast(context.?);
    const collector: *Collector = @ptrCast(aligned);
    return collector.nextRandom();
}

    fn handleTransientFailure(
        self: *Collector,
        handle: *SourceHandle,
        failure: LifecycleFailure,
        now: u128,
    ) void {
        _ = restart.acceptFailure(
            &handle.restart_state,
            handle.restart_policy,
            failure.kind,
            failure.permanence,
            now,
        );
        recordCounter(self.options, collector_metrics.restart_failures, 1);
        resetStreamErrorTracking(handle);
        self.refreshQuarantineGauge();
    }

    fn handlePermanentFailure(self: *Collector, handle: *SourceHandle, failure: LifecycleFailure) void {
        restart.noteFailure(&handle.restart_state, failure.kind, failure.permanence);
        handle.restart_state.pending_restart = false;
        handle.shutting_down = true;
        recordCounter(self.options, collector_metrics.restart_failures, 1);
        resetStreamErrorTracking(handle);
        updateActiveSourcesGauge(self);
        self.refreshQuarantineGauge();
    }

    fn refreshQuarantineGauge(self: *Collector) void {
        if (self.options.metrics) |metrics_sink| {
            const now = self.clock.now();
            var quarantined: usize = 0;
            for (self.sources.items) |handle| {
                if (restart.isUnderQuarantine(&handle.restart_state, now)) quarantined += 1;
            }
            if (math.cast(i64, quarantined)) |value| {
                metrics_sink.recordGauge(collector_metrics.restart_quarantine, value);
            }
        }
    }

    fn attemptHandleRestart(
        self: *Collector,
        handle: *SourceHandle,
        allocator: std.mem.Allocator,
        now: u128,
    ) void {
        if (!handle.instance.capabilities().streaming) {
            restart.recordSuccess(&handle.restart_state);
            handle.restart_state.pending_restart = false;
            self.refreshQuarantineGauge();
            return;
        }

        resetTransport(handle, allocator);

        var restart_failed = false;
        const maybe_stream = handle.instance.startStream(allocator) catch |err| {
            restart_failed = true;
            const failure = classifyStartError(err);
            if (failure.kind == .start_backpressure) {
                recordBackpressure(self);
            }
            logFailure(
                self.options,
                failure.severity,
                failure.permanence,
                "collector restart failure for {s}: {s}",
                .{ handle.descriptor.name, @errorName(err) },
            );

            if (failure.restartable and failure.permanence == .transient) {
                self.handleTransientFailure(handle, failure, now);
            } else {
                self.handlePermanentFailure(handle, failure);
            }

            return;
        };

        if (restart_failed) return;

        if (maybe_stream) |stream| {
            activateStreamTransport(handle, stream);
            restart.recordSuccess(&handle.restart_state);
            handle.restart_state.pending_restart = false;
            self.refreshQuarantineGauge();
            logInfo(self.options, "collector restarted stream for {s}", .{handle.descriptor.name});
            return;
        }

        if (!handle.supports_batching) {
            logError(
                self.options,
                "collector restart aborted: source {s} did not expose stream",
                .{handle.descriptor.name},
            );
            self.handlePermanentFailure(handle, .{
                .collector_error = CollectorError.StartFailed,
                .permanence = .permanent,
                .restartable = false,
                .severity = .err,
                .kind = .start_unknown,
            });
            return;
        }

        restart.recordSuccess(&handle.restart_state);
        handle.restart_state.pending_restart = false;
        activateBatchTransport(handle);
        self.refreshQuarantineGauge();
        logInfo(self.options, "collector restart using batch polling for {s}", .{handle.descriptor.name});
    }

    /// Attempts to fetch the next available batch from any active source. The
    /// round-robin schedule is preserved across calls.
    pub fn poll(self: *Collector, allocator: std.mem.Allocator) CollectorError!?PolledBatch {
        if (!self.started) return CollectorError.NotStarted;

        const total = self.sources.items.len;
        if (total == 0) return null;

        self.refreshQuarantineGauge();

        var last_attempted: ?usize = null;

        while (self.popReady()) |ready_index| {
            const outcome = try self.pollIndex(allocator, ready_index, total);
            last_attempted = ready_index;

            switch (outcome.status) {
                .yielded => {
                    self.next_index = outcome.next_index;
                    return outcome.batch;
                },
                .backpressure => {
                    self.next_index = outcome.next_index;
                    return CollectorError.Backpressure;
                },
                else => {},
            }
        }

        var index = if (self.next_index < total) self.next_index else 0;
        var examined: usize = 0;

        if (last_attempted) |value| {
            index = (value + 1) % total;
        }

        while (examined < total) : (examined += 1) {
            const outcome = try self.pollIndex(allocator, index, total);
            last_attempted = index;

            switch (outcome.status) {
                .yielded => {
                    self.next_index = outcome.next_index;
                    return outcome.batch;
                },
                .backpressure => {
                    self.next_index = outcome.next_index;
                    return CollectorError.Backpressure;
                },
                else => {},
            }

            index = outcome.next_index;
        }

        if (last_attempted) |value| {
            self.next_index = (value + 1) % total;
        } else {
            self.next_index = index;
        }

        return null;
    }

    fn tryPoll(
        self: *Collector,
        handle: *SourceHandle,
        allocator: std.mem.Allocator,
        now: u128,
    ) CollectorError!PollResult {
        switch (handle.transport) {
            .stream => |*stream_handle| return self.pollStreamTransport(handle, stream_handle, allocator, now),
            .batch => return self.pollBatchTransport(handle, allocator, now),
            .none => {
                if (handle.supports_batching) {
                    activateBatchTransport(handle);
                    return self.pollBatchTransport(handle, allocator, now);
                }

                return PollResult{ .status = .idle, .batch = null, .attempted = false };
            },
        }
    }

    fn pollStreamTransport(
        self: *Collector,
        handle: *SourceHandle,
        stream_handle: *event.EventStream,
        allocator: std.mem.Allocator,
        now: u128,
    ) CollectorError!PollResult {
        const batch = stream_handle.next(allocator) catch |err| switch (err) {
            error.EndOfStream => {
                resetTransport(handle, allocator);
                restart.recordSuccess(&handle.restart_state);
                if (handle.supports_batching) {
                    activateBatchTransport(handle);
                }
                resetStreamErrorTracking(handle);
                self.refreshQuarantineGauge();
                logInfo(self.options, "collector stream finished for {s}", .{handle.descriptor.name});
                return PollResult{ .status = .finished, .batch = null, .attempted = true };
            },
            else => {
                const failure = classifyStreamPollError(err);
                switch (failure.kind) {
                    .poll_backpressure => {
                        recordBackpressure(self);
                        logFailure(
                            self.options,
                            failure.severity,
                            failure.permanence,
                            "collector stream backpressure for {s}",
                            .{handle.descriptor.name},
                        );
                        return PollResult{ .status = .backpressure, .batch = null, .attempted = true };
                    },
                    else => {
                        recordSourcePollError(self, handle);
                        logFailure(
                            self.options,
                            failure.severity,
                            failure.permanence,
                            "collector stream failure for {s}: {s}",
                            .{ handle.descriptor.name, @errorName(err) },
                        );
                        switch (failure.kind) {
                            .poll_transport_failure, .poll_decode_failure => {
                                const should_quarantine = registerStreamError(handle, now);
                                resetTransport(handle, allocator);

                                if (should_quarantine) {
                                    self.handleTransientFailure(handle, failure, now);
                                    return PollResult{ .status = .idle, .batch = null, .attempted = false };
                                }

                                restart.noteFailure(&handle.restart_state, failure.kind, failure.permanence);
                                self.attemptHandleRestart(handle, allocator, now);
                                return PollResult{ .status = .idle, .batch = null, .attempted = false };
                            },
                            else => {
                                if (failure.restartable and failure.permanence == .transient) {
                                    resetTransport(handle, allocator);
                                    self.handleTransientFailure(handle, failure, now);
                                    return PollResult{ .status = .idle, .batch = null, .attempted = false };
                                }

                                if (failure.permanence == .permanent) {
                                    resetTransport(handle, allocator);
                                    self.handlePermanentFailure(handle, failure);
                                    return failure.collector_error;
                                }

                                resetTransport(handle, allocator);
                                return PollResult{ .status = .idle, .batch = null, .attempted = false };
                            },
                        }
                    },
                }
            },
        };

        if (batch) |value| {
            resetStreamErrorTracking(handle);
            restart.recordSuccess(&handle.restart_state);
            self.refreshQuarantineGauge();
            return PollResult{ .status = .yielded, .batch = value, .attempted = true };
        }

        resetStreamErrorTracking(handle);
        return PollResult{ .status = .empty, .batch = null, .attempted = true };
    }

    fn pollBatchTransport(
        self: *Collector,
        handle: *SourceHandle,
        allocator: std.mem.Allocator,
        now: u128,
    ) CollectorError!PollResult {
        std.debug.assert(handle.supports_batching);
        const batch = handle.instance.pollBatch(allocator) catch |err| {
            const failure = classifyBatchPollError(err);
            switch (failure.kind) {
                .poll_backpressure => {
                    recordBackpressure(self);
                    logFailure(
                        self.options,
                        failure.severity,
                        failure.permanence,
                        "collector batch backpressure for {s}",
                        .{handle.descriptor.name},
                    );
                    return PollResult{ .status = .backpressure, .batch = null, .attempted = true };
                },
                .poll_invalid_configuration => {
                    logFailure(
                        self.options,
                        failure.severity,
                        failure.permanence,
                        "collector invalid configuration during batch poll for {s}: {s}",
                        .{ handle.descriptor.name, @errorName(err) },
                    );
                    self.handlePermanentFailure(handle, failure);
                    return failure.collector_error;
                },
                else => {
                    recordPollError(self);
                    logFailure(
                        self.options,
                        failure.severity,
                        failure.permanence,
                        "collector batch failure for {s}: {s}",
                        .{ handle.descriptor.name, @errorName(err) },
                    );

                    if (failure.restartable and failure.permanence == .transient) {
                        self.handleTransientFailure(handle, failure, now);
                        return PollResult{ .status = .idle, .batch = null, .attempted = false };
                    }

                    if (failure.permanence == .permanent) {
                        self.handlePermanentFailure(handle, failure);
                        return failure.collector_error;
                    }

                    return PollResult{ .status = .idle, .batch = null, .attempted = false };
                },
            }
        };

        if (batch) |value| {
            restart.recordSuccess(&handle.restart_state);
            self.refreshQuarantineGauge();
            return PollResult{ .status = .yielded, .batch = value, .attempted = true };
        }

        return PollResult{ .status = .empty, .batch = null, .attempted = true };
    }

    /// Shuts down all sources and resets collector bookkeeping.
    pub fn shutdown(self: *Collector, allocator: std.mem.Allocator) CollectorError!void {
        destroySourceHandles(self, allocator);
        self.started = false;
        self.next_index = 0;
        self.clearReadyQueue();
        updateActiveSourcesGauge(self);
        logInfo(self.options, "collector shutdown complete", .{});
    }

    fn enqueueReady(self: *Collector, index: usize) void {
        if (index >= self.sources.items.len) return;
        var handle = &self.sources.items[index];
        if (handle.shutting_down) return;
        if (handle.ready_signal_depth >= max_ready_queue_depth) return;

        handle.ready_signal_depth += 1;
        self.ready_queue.append(self.allocator, index) catch {
            handle.ready_signal_depth -= 1;
            logError(
                self.options,
                "collector ready queue append failed for {s}",
                .{handle.descriptor.name},
            );
        };
    }

    fn popReady(self: *Collector) ?usize {
        while (self.ready_queue_head < self.ready_queue.items.len) {
            const index = self.ready_queue.items[self.ready_queue_head];
            self.ready_queue_head += 1;
            if (index >= self.sources.items.len) continue;

            var handle = &self.sources.items[index];
            if (handle.ready_signal_depth > 0) {
                handle.ready_signal_depth -= 1;
            }
            if (handle.shutting_down) continue;
            return index;
        }

        if (self.ready_queue_head >= self.ready_queue.items.len and self.ready_queue.items.len != 0) {
            self.ready_queue.clearRetainingCapacity();
            self.ready_queue_head = 0;
        }

        return null;
    }

    fn clearReadyQueue(self: *Collector) void {
        if (self.ready_queue.items.len == 0) {
            self.ready_queue_head = 0;
            return;
        }

        // Reset pending markers for any indices that remain queued.
        var idx: usize = self.ready_queue_head;
        while (idx < self.ready_queue.items.len) : (idx += 1) {
            const index = self.ready_queue.items[idx];
            if (index >= self.sources.items.len) continue;
            self.sources.items[index].ready_signal_depth = 0;
        }

        self.ready_queue.clearRetainingCapacity();
        self.ready_queue_head = 0;
    }

    fn registerReadyObserver(self: *Collector, handle: *SourceHandle, index: usize) void {
        if (!handle.instance.supportsReadyObserver()) {
            handle.ready_signal_registered = false;
            return;
        }
        var context_ptr = handle.ready_signal_context;
        if (context_ptr == null) {
            const allocated = self.allocator.create(ReadySignalContext) catch {
                logWarn(
                    self.options,
                    "collector ready observer allocation failed for {s}",
                    .{handle.descriptor.name},
                );
                handle.ready_signal_registered = false;
                return;
            };
            handle.ready_signal_context = allocated;
            context_ptr = allocated;
        }

        const ctx = context_ptr.?;
        ctx.* = .{ .collector = self, .index = index };

        const observer = source.ReadyObserver{
            .context = @as(*anyopaque, @ptrCast(ctx)),
            .notify_fn = ReadySignalContext.notify,
        };

        handle.instance.registerReadyObserver(observer);
        handle.ready_signal_registered = true;
    }

    fn applyProductiveBonus(self: *Collector, index: usize, streak: usize) void {
        if (streak == 0) return;

        var bonus = streak;
        while (bonus > 0) : (bonus -= 1) {
            self.enqueueReady(index);
        }
    }

    fn pollIndex(self: *Collector, allocator: std.mem.Allocator, index: usize, total: usize) CollectorError!PollIndexResult {
        const next_index = (index + 1) % total;
        var handle = &self.sources.items[index];

        if (handle.shutting_down) {
            return PollIndexResult{ .status = .skipped, .next_index = next_index };
        }

        const now = self.clock.now();

        if (restart.isUnderQuarantine(&handle.restart_state, now)) {
            return PollIndexResult{ .status = .skipped, .next_index = next_index };
        }

        if (handle.restart_state.pending_restart) {
            if (now >= handle.restart_state.backoff.resume_at_ns) {
                self.attemptHandleRestart(handle, allocator, now);
            }
            if (handle.restart_state.pending_restart) {
                return PollIndexResult{ .status = .skipped, .next_index = next_index };
            }
        }

        const ready = handle.instance.readyHint();
        const forced_poll = !ready and handle.ready_skip_count >= max_ready_skip_cycles;
        if (!ready and !forced_poll) {
            if (handle.ready_skip_count < math.maxInt(usize)) {
                handle.ready_skip_count += 1;
            }
            recordReadySkip(self);
            if (handle.productive_streak > 0) {
                handle.productive_streak -= 1;
            }
            return PollIndexResult{ .status = .skipped, .next_index = next_index };
        }
        if (forced_poll) {
            logInfo(
                self.options,
                "collector forcing poll for {s} after {d} ready skips",
                .{ handle.descriptor.name, handle.ready_skip_count },
            );
        }
        handle.ready_skip_count = 0;

        const result = try self.tryPoll(handle, allocator, now);

        switch (result.status) {
            .yielded => {
                const batch = result.batch orelse unreachable;
                handle.consecutive_empty_polls = 0;
                if (handle.productive_streak < max_productive_streak) {
                    handle.productive_streak += 1;
                }
                self.applyProductiveBonus(index, handle.productive_streak);
                recordBatchMetrics(self, batch.events.len);
                return PollIndexResult{
                    .status = .yielded,
                    .next_index = next_index,
                    .batch = PolledBatch{
                        .descriptor = handle.descriptor,
                        .batch = batch,
                    },
                };
            },
            .backpressure => return PollIndexResult{ .status = .backpressure, .next_index = next_index },
            else => {},
        }

        if (result.attempted and result.status != .yielded and result.status != .backpressure) {
            if (handle.consecutive_empty_polls < math.maxInt(usize)) {
                handle.consecutive_empty_polls += 1;
            }
            recordEmptyPoll(self);
            if (handle.productive_streak > 0) {
                handle.productive_streak -= 1;
            }
        }

        return PollIndexResult{ .status = .idle, .next_index = next_index };
    }
};

fn destroySourceHandles(collector: *Collector, allocator: std.mem.Allocator) void {
    if (collector.sources.items.len == 0) return;

    for (collector.sources.items) |*handle| {
        if (!handle.shutting_down) {
            resetTransport(handle, allocator);
            handle.instance.shutdown(allocator);
            handle.shutting_down = true;
        }

        handle.ready_signal_depth = 0;
        handle.ready_signal_registered = false;
        handle.productive_streak = 0;
        resetStreamErrorTracking(handle);

        if (handle.poll_error_counter_name.len != 0) {
            allocator.free(handle.poll_error_counter_name);
            handle.poll_error_counter_name = "";
        }

        if (handle.ready_signal_context) |context| {
            allocator.destroy(context);
            handle.ready_signal_context = null;
        }
    }
}

fn selectRestartPolicy(descriptor: source.SourceDescriptor) RestartPolicy {
    return switch (descriptor.type) {
        .syslog => syslog_restart_policy,
    };
}

fn deriveSeed(clock: Clock, salt: usize) u64 {
    var seed: u64 = @truncate(clock.now());
    seed ^= @truncate(salt);
    if (seed == 0) seed = 0x6d5f_ea2c_8c4a_f3b1;
    return seed;
}

fn classifyStartError(err: source.SourceError) LifecycleFailure {
    return switch (err) {
        error.InvalidConfiguration => .{
            .collector_error = CollectorError.InvalidConfiguration,
            .permanence = .permanent,
            .restartable = false,
            .severity = .err,
            .kind = .start_invalid_configuration,
        },
        error.OperationNotSupported => .{
            .collector_error = CollectorError.InvalidConfiguration,
            .permanence = .permanent,
            .restartable = false,
            .severity = .err,
            .kind = .start_unsupported,
        },
        error.NotImplemented => .{
            .collector_error = CollectorError.Unimplemented,
            .permanence = .permanent,
            .restartable = false,
            .severity = .err,
            .kind = .start_not_implemented,
        },
        error.Backpressure => .{
            .collector_error = CollectorError.Backpressure,
            .permanence = .transient,
            .restartable = true,
            .severity = .warn,
            .kind = .start_backpressure,
        },
        error.StartupFailed => .{
            .collector_error = CollectorError.StartFailed,
            .permanence = .transient,
            .restartable = true,
            .severity = .err,
            .kind = .start_startup_failed,
        },
        error.ShutdownFailed => .{
            .collector_error = CollectorError.ShutdownFailed,
            .permanence = .permanent,
            .restartable = false,
            .severity = .err,
            .kind = .start_unknown,
        },
    };
}

fn classifyStreamPollError(err: event.StreamError) LifecycleFailure {
    return switch (err) {
        error.TransportFailure => .{
            .collector_error = CollectorError.PollFailed,
            .permanence = .transient,
            .restartable = true,
            .severity = .err,
            .kind = .poll_transport_failure,
        },
        error.DecodeFailure => .{
            .collector_error = CollectorError.PollFailed,
            .permanence = .transient,
            .restartable = true,
            .severity = .err,
            .kind = .poll_decode_failure,
        },
        error.Backpressure => .{
            .collector_error = CollectorError.Backpressure,
            .permanence = .transient,
            .restartable = false,
            .severity = .warn,
            .kind = .poll_backpressure,
        },
        else => unreachable,
    };
}

fn classifyBatchPollError(err: source.SourceError) LifecycleFailure {
    return switch (err) {
        error.Backpressure => .{
            .collector_error = CollectorError.Backpressure,
            .permanence = .transient,
            .restartable = false,
            .severity = .warn,
            .kind = .poll_backpressure,
        },
        error.InvalidConfiguration, error.OperationNotSupported => .{
            .collector_error = CollectorError.InvalidConfiguration,
            .permanence = .permanent,
            .restartable = false,
            .severity = .err,
            .kind = .poll_invalid_configuration,
        },
        else => .{
            .collector_error = CollectorError.PollFailed,
            .permanence = .transient,
            .restartable = true,
            .severity = .err,
            .kind = .poll_unknown,
        },
    };
}

fn mapStartError(self: *Collector, descriptor: source.SourceDescriptor, err: source.SourceError) CollectorError {
    const failure = classifyStartError(err);
    if (failure.kind == .start_backpressure) {
        recordBackpressure(self);
    }
    logFailure(
        self.options,
        failure.severity,
        failure.permanence,
        "collector start failure for {s}: {s}",
        .{ descriptor.name, @errorName(err) },
    );
    return failure.collector_error;
}

fn updateActiveSourcesGauge(self: *Collector) void {
    if (self.options.metrics) |metrics_sink| {
        var active: usize = 0;
        var failed: usize = 0;
        for (self.sources.items) |handle| {
            if (!handle.shutting_down) {
                active += 1;
            } else {
                failed += 1;
            }
        }

        if (math.cast(i64, active)) |value| {
            metrics_sink.recordGauge(collector_metrics.active_sources, value);
        }
        if (math.cast(i64, failed)) |value| {
            metrics_sink.recordGauge(collector_metrics.failed_sources, value);
        }
    }
}

fn recordBatchMetrics(self: *Collector, batch_size: usize) void {
    recordCounter(self.options, collector_metrics.batches, 1);
    const events_count: u64 = @intCast(batch_size);
    recordCounter(self.options, collector_metrics.events, events_count);
}

fn recordBackpressure(self: *Collector) void {
    recordCounter(self.options, collector_metrics.backpressure, 1);
}

fn recordPollError(self: *Collector) void {
    recordCounter(self.options, collector_metrics.poll_errors, 1);
}

fn recordSourcePollError(self: *Collector, handle: *SourceHandle) void {
    recordPollError(self);

    const metrics_sink = self.options.metrics orelse return;

    if (handle.poll_error_counter_name.len == 0) {
        handle.poll_error_counter_name = std.fmt.allocPrint(
            self.allocator,
            "{s}_source_{s}",
            .{ collector_metrics.poll_errors, handle.descriptor.name },
        ) catch |
            err|
        {
            logWarn(
                self.options,
                "collector failed to allocate poll error metric for {s}: {s}",
                .{ handle.descriptor.name, @errorName(err) },
            );
            return;
        };
    }

    metrics_sink.incrCounter(handle.poll_error_counter_name, 1);
}

fn recordReadySkip(self: *Collector) void {
    recordCounter(self.options, collector_metrics.ready_skips, 1);
}

fn recordEmptyPoll(self: *Collector) void {
    recordCounter(self.options, collector_metrics.empty_polls, 1);
}

const TestClock = struct {
    now_ns: u128,

    fn now(context: ?*anyopaque) u128 {
        const aligned: *align(@alignOf(TestClock)) anyopaque = @alignCast(context.?);
        const self: *TestClock = @ptrCast(aligned);
        return self.now_ns;
    }
};

test "collector init fails without matching factory" {
    const testing = std.testing;

    const config = cfg.SourceConfig{
        .id = "missing_source",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const empty_registry = source.Registry{ .factories = &[_]source.SourceFactory{} };
    const options = CollectorOptions{ .registry = empty_registry };

    try testing.expectError(
        CollectorError.UnsupportedSource,
        Collector.init(testing.allocator, &.{config}, options),
    );
}

test "collector polls batch-only mock source" {
    const testing = std.testing;

    const Mock = struct {
        pub const Harness = struct {
            shutdowns: usize = 0,
        };

        pub var harness: ?*Harness = null;

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
            emitted: bool = false,
            event_storage: [1]event.Event,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const descriptor = source.SourceDescriptor{
                .type = .syslog,
                .name = config.id,
            };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
                .event_storage = .{event.Event{
                    .metadata = .{ .source_id = config.id },
                    .payload = .{ .log = .{ .message = "hello", .fields = &[_]event.Field{} } },
                }},
            };

            const lifecycle = source.BatchLifecycle{
                .poll_batch = pollBatch,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .batch = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = false, .batching = true },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            _ = context;
            _ = allocator;
            return source.SourceError.OperationNotSupported;
        }

        fn pollBatch(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            if (state.emitted) return null;
            state.emitted = true;

            return event.EventBatch{
                .events = state.event_storage[0..1],
                .ack = event.AckToken.none(),
            };
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.harness.shutdowns += 1;
            state.allocator.destroy(state);
        }

        fn readyHint(context: *anyopaque) bool {
            const state = asState(context);
            return !state.emitted;
        }
    };

    var harness = Mock.Harness{};
    Mock.harness = &harness;
    defer Mock.harness = null;

    const config = cfg.SourceConfig{
        .id = "mock_source",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);

    const stats = collector.status();
    try testing.expect(stats.started);
    try testing.expectEqual(@as(usize, 1), stats.active_sources);
    try testing.expectEqual(@as(usize, 0), stats.streaming_sources);

    const maybe_batch = try collector.poll(testing.allocator);
    try testing.expect(maybe_batch != null);
    const polled = maybe_batch.?;
    try testing.expectEqualStrings("mock_source", polled.descriptor.name);
    try testing.expectEqual(@as(usize, 1), polled.batch.events.len);
    const payload = polled.batch.events[0].payload;
    switch (payload) {
        .log => |log_event| {
            try testing.expectEqualStrings("hello", log_event.message);
        },
    }

    const second = try collector.poll(testing.allocator);
    try testing.expect(second == null);

    try collector.shutdown(testing.allocator);
    try testing.expectEqual(@as(usize, 1), harness.shutdowns);

    const after_stats = collector.status();
    try testing.expect(!after_stats.started);
    try testing.expectEqual(@as(usize, 0), after_stats.active_sources);
}

test "collector defers polling until ready hint satisfied" {
    const testing = std.testing;

    const Mock = struct {
        pub const Harness = struct {
            shutdowns: usize = 0,
            poll_calls: usize = 0,
            ready_calls: usize = 0,
        };

        pub var harness: ?*Harness = null;

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
            emitted: bool = false,
            event_storage: [1]event.Event,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const descriptor = source.SourceDescriptor{
                .type = .syslog,
                .name = config.id,
            };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
                .event_storage = .{event.Event{
                    .metadata = .{ .source_id = config.id },
                    .payload = .{ .log = .{ .message = "ready", .fields = &[_]event.Field{} } },
                }},
            };

            const lifecycle = source.BatchLifecycle{
                .poll_batch = pollBatch,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .batch = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = false, .batching = true },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            _ = context;
            _ = allocator;
            return source.SourceError.OperationNotSupported;
        }

        fn pollBatch(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            state.harness.poll_calls += 1;
            if (state.emitted) return null;
            state.emitted = true;
            return event.EventBatch{
                .events = state.event_storage[0..1],
                .ack = event.AckToken.none(),
            };
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.harness.shutdowns += 1;
            state.allocator.destroy(state);
        }

        fn readyHint(context: *anyopaque) bool {
            const state = asState(context);
            state.harness.ready_calls += 1;
            return state.harness.ready_calls > max_ready_skip_cycles;
        }
    };

    var harness = Mock.Harness{};
    Mock.harness = &harness;
    defer Mock.harness = null;

    const config = cfg.SourceConfig{
        .id = "ready_source",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);

    var attempts: usize = 0;
    while (attempts < max_ready_skip_cycles) : (attempts += 1) {
        const maybe_batch = try collector.poll(testing.allocator);
        try testing.expect(maybe_batch == null);
        try testing.expectEqual(@as(usize, 0), harness.poll_calls);
    }

    const produced = try collector.poll(testing.allocator);
    try testing.expect(produced != null);
    try testing.expectEqual(@as(usize, 1), harness.poll_calls);
    try testing.expectEqual(@as(usize, max_ready_skip_cycles + 1), harness.ready_calls);

    try collector.shutdown(testing.allocator);
    try testing.expectEqual(@as(usize, 1), harness.shutdowns);
}

test "collector biases scheduling toward productive sources" {
    const testing = std.testing;

    const Mock = struct {
        pub const Harness = struct {
            allocator: std.mem.Allocator,
            sequence: std.ArrayList(u8),
            a_poll_calls: usize = 0,
            b_poll_calls: usize = 0,
            a_ready_calls: usize = 0,
            b_ready_calls: usize = 0,

            fn init(allocator: std.mem.Allocator) Harness {
                return .{ .allocator = allocator, .sequence = std.ArrayList(u8).init(allocator) };
            }

            fn deinit(self: *Harness) void {
                self.sequence.deinit();
            }
        };

        pub var harness: ?*Harness = null;

        const Role = enum { productive, idle };

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
            role: Role,
            has_data: bool,
            event_storage: [1]event.Event,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const role = if (std.mem.eql(u8, config.id, "a_source")) Role.productive else Role.idle;

            const descriptor = source.SourceDescriptor{
                .type = .syslog,
                .name = config.id,
            };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
                .role = role,
                .has_data = role == .productive,
                .event_storage = .{event.Event{
                    .metadata = .{ .source_id = config.id },
                    .payload = .{ .log = .{ .message = config.id, .fields = &[_]event.Field{} } },
                }},
            };

            const lifecycle = source.BatchLifecycle{
                .poll_batch = pollBatch,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .batch = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = false, .batching = true },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            _ = context;
            _ = allocator;
            return source.SourceError.OperationNotSupported;
        }

        fn pollBatch(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            switch (state.role) {
                .productive => {
                    state.harness.a_poll_calls += 1;
                    if (!state.has_data) return null;
                    state.has_data = false;
                    return event.EventBatch{ .events = state.event_storage[0..1], .ack = event.AckToken.none() };
                },
                .idle => {
                    state.harness.b_poll_calls += 1;
                    return null;
                },
            }
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.allocator.destroy(state);
        }

        fn readyHint(context: *anyopaque) bool {
            const state = asState(context);
            switch (state.role) {
                .productive => state.harness.a_ready_calls += 1,
                .idle => state.harness.b_ready_calls += 1,
            }
            const marker: u8 = if (state.role == .productive) 'A' else 'B';
            state.harness.sequence.append(marker) catch {};
            return state.has_data;
        }
    };

    var harness_storage = Mock.Harness.init(testing.allocator);
    defer harness_storage.deinit();
    Mock.harness = &harness_storage;
    defer Mock.harness = null;

    const configs = [_]cfg.SourceConfig{
        .{ .id = "a_source", .payload = .{ .syslog = .{ .address = "127.0.0.1:1514" } } },
        .{ .id = "b_source", .payload = .{ .syslog = .{ .address = "127.0.0.1:2514" } } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry };

    var collector = try Collector.init(testing.allocator, &configs, options);
    defer collector.deinit();
    defer collector.shutdown(testing.allocator) catch {};

    try collector.start(testing.allocator);

    const first = try collector.poll(testing.allocator);
    try testing.expect(first != null);
    try testing.expectEqualStrings("a_source", first.?.descriptor.name);

    // Second poll should prioritise the productive source again via weighting.
    const second = try collector.poll(testing.allocator);
    try testing.expect(second == null);

    try testing.expect(harness_storage.sequence.items.len >= 2);
    try testing.expectEqual(@as(u8, 'A'), harness_storage.sequence.items[0]);
    try testing.expectEqual(@as(u8, 'A'), harness_storage.sequence.items[1]);
    try testing.expectEqual(@as(usize, 1), harness_storage.a_poll_calls);
}

test "collector forces poll after exhausting ready hint budget" {
    const testing = std.testing;

    const MetricsHarness = struct {
        ready_skips: usize = 0,
        empty_polls: usize = 0,

        fn incrCounter(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (std.mem.eql(u8, name, collector_metrics.ready_skips)) {
                self.ready_skips += @as(usize, @intCast(value));
                return;
            }
            if (std.mem.eql(u8, name, collector_metrics.empty_polls)) {
                self.empty_polls += @as(usize, @intCast(value));
            }
        }
    };

    const Mock = struct {
        pub const Harness = struct {
            shutdowns: usize = 0,
            poll_calls: usize = 0,
            ready_calls: usize = 0,
        };

        pub var harness: ?*Harness = null;

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const descriptor = source.SourceDescriptor{
                .type = .syslog,
                .name = config.id,
            };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
            };

            const lifecycle = source.BatchLifecycle{
                .poll_batch = pollBatch,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .batch = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = false, .batching = true },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            _ = context;
            _ = allocator;
            return source.SourceError.OperationNotSupported;
        }

        fn pollBatch(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            state.harness.poll_calls += 1;
            return null;
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.harness.shutdowns += 1;
            state.allocator.destroy(state);
        }

        fn readyHint(context: *anyopaque) bool {
            const state = asState(context);
            state.harness.ready_calls += 1;
            return false;
        }
    };

    var source_harness = Mock.Harness{};
    Mock.harness = &source_harness;
    defer Mock.harness = null;

    var metrics_harness = MetricsHarness{};
    var metrics_impl = source.Metrics{
        .context = &metrics_harness,
        .incr_counter_fn = MetricsHarness.incrCounter,
        .record_gauge_fn = null,
    };

    const config = cfg.SourceConfig{
        .id = "forced_source",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry, .metrics = &metrics_impl };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);

    var attempts: usize = 0;
    while (attempts < max_ready_skip_cycles) : (attempts += 1) {
        const result = try collector.poll(testing.allocator);
        try testing.expect(result == null);
        try testing.expectEqual(@as(usize, 0), source_harness.poll_calls);
    }

    try testing.expectEqual(@as(usize, max_ready_skip_cycles), metrics_harness.ready_skips);

    const forced = try collector.poll(testing.allocator);
    try testing.expect(forced == null);
    try testing.expectEqual(@as(usize, 1), source_harness.poll_calls);
    try testing.expectEqual(@as(usize, max_ready_skip_cycles + 1), source_harness.ready_calls);
    try testing.expectEqual(@as(usize, 1), metrics_harness.empty_polls);

    try collector.shutdown(testing.allocator);
    try testing.expectEqual(@as(usize, 1), source_harness.shutdowns);
}

test "collector surfaces stream backpressure error" {
    const testing = std.testing;

    const MetricsHarness = struct {
        backpressure: usize = 0,

        fn incrCounter(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (std.mem.eql(u8, name, collector_metrics.backpressure)) {
                self.backpressure += @as(usize, @intCast(value));
            }
        }
    };

    const Mock = struct {
        pub const Harness = struct {
            shutdowns: usize = 0,
            finish_calls: usize = 0,
            next_calls: usize = 0,
        };

        pub var harness: ?*Harness = null;

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const descriptor = source.SourceDescriptor{
                .type = .syslog,
                .name = config.id,
            };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
            };

            const lifecycle = source.StreamLifecycle{
                .start_stream = startStream,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .stream = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = true, .batching = false },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            const state = asState(context);
            _ = allocator;
            return event.EventStream{
                .context = state,
                .next_fn = streamNext,
                .finish_fn = streamFinish,
            };
        }

        fn streamNext(context: *anyopaque, allocator: std.mem.Allocator) event.StreamError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            state.harness.next_calls += 1;
            return error.Backpressure;
        }

        fn streamFinish(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.harness.finish_calls += 1;
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            const state = asState(context);
            state.harness.shutdowns += 1;
            state.allocator.destroy(state);
            _ = allocator;
        }

        fn readyHint(context: *anyopaque) bool {
            _ = context;
            return true;
        }
    };

    var source_harness = Mock.Harness{};
    Mock.harness = &source_harness;
    defer Mock.harness = null;

    var metrics_harness = MetricsHarness{};
    var metrics_impl = source.Metrics{
        .context = &metrics_harness,
        .incr_counter_fn = MetricsHarness.incrCounter,
        .record_gauge_fn = null,
    };

    const config = cfg.SourceConfig{
        .id = "stream_backpressure",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry, .metrics = &metrics_impl };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);

    try testing.expectError(CollectorError.Backpressure, collector.poll(testing.allocator));
    try testing.expectEqual(@as(usize, 1), metrics_harness.backpressure);
    try testing.expectEqual(@as(usize, 0), source_harness.finish_calls);
    try testing.expectEqual(@as(usize, 1), source_harness.next_calls);

    try collector.shutdown(testing.allocator);
    try testing.expectEqual(@as(usize, 1), source_harness.shutdowns);
}

test "collector releases stream after end of stream" {
    const testing = std.testing;

    const Mock = struct {
        pub const Harness = struct {
            finish_calls: usize = 0,
            shutdowns: usize = 0,
        };

        pub var harness: ?*Harness = null;

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
            emitted: bool = false,
            event_storage: [1]event.Event,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const descriptor = source.SourceDescriptor{
                .type = .syslog,
                .name = config.id,
            };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
                .event_storage = .{event.Event{
                    .metadata = .{ .source_id = config.id },
                    .payload = .{ .log = .{ .message = "payload", .fields = &[_]event.Field{} } },
                }},
            };

            const lifecycle = source.StreamLifecycle{
                .start_stream = startStream,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .stream = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = true, .batching = false },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            const state = asState(context);
            _ = allocator;
            return event.EventStream{
                .context = state,
                .next_fn = streamNext,
                .finish_fn = streamFinish,
            };
        }

        fn streamNext(context: *anyopaque, allocator: std.mem.Allocator) event.StreamError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            if (state.emitted) {
                return error.EndOfStream;
            }
            state.emitted = true;
            return event.EventBatch{
                .events = state.event_storage[0..1],
                .ack = event.AckToken.none(),
            };
        }

        fn streamFinish(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.harness.finish_calls += 1;
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            const state = asState(context);
            state.harness.shutdowns += 1;
            state.allocator.destroy(state);
            _ = allocator;
        }

        fn readyHint(context: *anyopaque) bool {
            _ = context;
            return true;
        }
    };

    var harness = Mock.Harness{};
    Mock.harness = &harness;
    defer Mock.harness = null;

    const config = cfg.SourceConfig{
        .id = "stream_end",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);

    const first = try collector.poll(testing.allocator);
    try testing.expect(first != null);
    try testing.expectEqual(@as(usize, 1), collector.status().streaming_sources);

    const second = try collector.poll(testing.allocator);
    try testing.expect(second == null);
    try testing.expectEqual(@as(usize, 0), collector.status().streaming_sources);
    try testing.expectEqual(@as(usize, 1), harness.finish_calls);

    try collector.shutdown(testing.allocator);
    try testing.expectEqual(@as(usize, 1), harness.shutdowns);
}

test "collector stream decode failures trigger backoff after streak" {
    const testing = std.testing;

    const MetricsHarness = struct {
        total: usize = 0,
        per_source: usize = 0,
        last_name: []const u8 = "",

        fn incrCounter(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            const delta: usize = @intCast(value);

            if (std.mem.eql(u8, name, collector_metrics.poll_errors)) {
                self.total += delta;
                return;
            }

            if (std.mem.startsWith(u8, name, collector_metrics.poll_errors)) {
                self.per_source += delta;
                self.last_name = name;
            }
        }
    };

    const Mock = struct {
        pub const Harness = struct {
            start_calls: usize = 0,
            next_calls: usize = 0,
            finish_calls: usize = 0,
        };

        pub var harness: ?*Harness = null;

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const descriptor = source.SourceDescriptor{ .type = .syslog, .name = config.id };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
            };

            const lifecycle = source.StreamLifecycle{
                .start_stream = startStream,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .stream = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = true, .batching = false },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            const state = asState(context);
            _ = allocator;
            state.harness.start_calls += 1;
            return event.EventStream{
                .context = state,
                .next_fn = streamNext,
                .finish_fn = streamFinish,
            };
        }

        fn streamNext(context: *anyopaque, allocator: std.mem.Allocator) event.StreamError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            state.harness.next_calls += 1;
            return event.StreamError.DecodeFailure;
        }

        fn streamFinish(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.harness.finish_calls += 1;
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            const state = asState(context);
            state.allocator.destroy(state);
            _ = allocator;
        }

        fn readyHint(context: *anyopaque) bool {
            _ = context;
            return true;
        }
    };

    const ClockHarness = struct {
        now_value: u128 = 0,

        fn now(context: ?*anyopaque) u128 {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            return self.now_value;
        }
    };

    var metrics_harness = MetricsHarness{};
    var metrics_impl = source.Metrics{
        .context = &metrics_harness,
        .incr_counter_fn = MetricsHarness.incrCounter,
        .record_gauge_fn = null,
    };

    var source_harness = Mock.Harness{};
    Mock.harness = &source_harness;
    defer Mock.harness = null;

    var clock_state = ClockHarness{};
    const custom_clock = Clock{ .context = &clock_state, .now_fn = ClockHarness.now };

    const config = cfg.SourceConfig{
        .id = "decode_failures",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{
        .registry = registry,
        .metrics = &metrics_impl,
        .restart = .{ .clock = custom_clock, .rng_seed = 42 },
    };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);

    const handle = &collector.sources.items[0];

    inline for (0..max_stream_error_streak) |idx| {
        clock_state.now_value += 1_000;
        const polled = try collector.poll(testing.allocator);
        try testing.expect(polled == null);
        try testing.expectEqual(@as(usize, idx + 1), metrics_harness.total);
        try testing.expectEqual(@as(usize, idx + 1), metrics_harness.per_source);

        if (idx < max_stream_error_streak - 1) {
            try testing.expect(!handle.restart_state.pending_restart);
        }
    }

    try testing.expect(handle.restart_state.pending_restart);
    try testing.expect(metrics_harness.last_name.len != 0);
    try testing.expect(source_harness.finish_calls >= max_stream_error_streak);

    try collector.shutdown(testing.allocator);
}

test "collector falls back to batch after stream completion" {
    const testing = std.testing;

    const Mock = struct {
        pub const Harness = struct {
            finish_calls: usize = 0,
            batch_polls: usize = 0,
            shutdowns: usize = 0,
        };

        pub var harness: ?*Harness = null;

        const State = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
            harness: *Harness,
            stream_emitted: bool = false,
            batch_emitted: bool = false,
            stream_storage: [1]event.Event,
            batch_storage: [1]event.Event,
        };

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const h = harness orelse return source.SourceError.InvalidConfiguration;

            const descriptor = source.SourceDescriptor{
                .type = .syslog,
                .name = config.id,
            };

            const state = ctx.allocator.create(State) catch return source.SourceError.StartupFailed;
            state.* = .{
                .allocator = ctx.allocator,
                .descriptor = descriptor,
                .harness = h,
                .stream_storage = .{event.Event{
                    .metadata = .{ .source_id = config.id },
                    .payload = .{ .log = .{ .message = "stream-event", .fields = &[_]event.Field{} } },
                }},
                .batch_storage = .{event.Event{
                    .metadata = .{ .source_id = config.id },
                    .payload = .{ .log = .{ .message = "batch-event", .fields = &[_]event.Field{} } },
                }},
            };

            const stream_lifecycle = source.StreamLifecycle{
                .start_stream = startStream,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            const batch_lifecycle = source.BatchLifecycle{
                .poll_batch = pollBatch,
                .shutdown = shutdown,
                .ready_hint = readyHint,
            };

            return source.Source{
                .stream = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = true, .batching = true },
                    .context = state,
                    .lifecycle = stream_lifecycle,
                    .batching = .{ .supported = batch_lifecycle },
                },
            };
        }

        fn asState(ptr: *anyopaque) *State {
            const aligned: *align(@alignOf(State)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            _ = allocator;
            const state = asState(context);
            return event.EventStream{
                .context = state,
                .next_fn = streamNext,
                .finish_fn = streamFinish,
            };
        }

        fn streamNext(context: *anyopaque, allocator: std.mem.Allocator) event.StreamError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            if (state.stream_emitted) {
                return error.EndOfStream;
            }
            state.stream_emitted = true;
            return event.EventBatch{
                .events = state.stream_storage[0..1],
                .ack = event.AckToken.none(),
            };
        }

        fn streamFinish(context: *anyopaque, allocator: std.mem.Allocator) void {
            _ = allocator;
            const state = asState(context);
            state.harness.finish_calls += 1;
        }

        fn pollBatch(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventBatch {
            _ = allocator;
            const state = asState(context);
            if (state.batch_emitted) {
                return null;
            }
            state.batch_emitted = true;
            state.harness.batch_polls += 1;
            return event.EventBatch{
                .events = state.batch_storage[0..1],
                .ack = event.AckToken.none(),
            };
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            const state = asState(context);
            state.harness.shutdowns += 1;
            state.allocator.destroy(state);
            _ = allocator;
        }

        fn readyHint(context: *anyopaque) bool {
            _ = context;
            return true;
        }
    };

    var harness = Mock.Harness{};
    Mock.harness = &harness;
    defer Mock.harness = null;

    const config = cfg.SourceConfig{
        .id = "stream_to_batch",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);

    const first = try collector.poll(testing.allocator);
    try testing.expect(first != null);
    try testing.expectEqualStrings("stream-event", first.?.batch.events[0].payload.log.message);
    try testing.expectEqual(@as(usize, 1), collector.status().streaming_sources);
    try testing.expectEqual(@as(usize, 0), harness.finish_calls);

    const handle = &collector.sources.items[0];

    const second = try collector.poll(testing.allocator);
    try testing.expect(second == null);
    try testing.expectEqual(@as(usize, 1), harness.finish_calls);
    try testing.expect(!transportIsStreaming(handle));
    try testing.expectEqual(@as(usize, 0), collector.status().streaming_sources);

    const third = try collector.poll(testing.allocator);
    try testing.expect(third != null);
    try testing.expectEqualStrings("batch-event", third.?.batch.events[0].payload.log.message);
    try testing.expectEqual(@as(usize, 1), harness.batch_polls);

    const fourth = try collector.poll(testing.allocator);
    try testing.expect(fourth == null);

    try collector.shutdown(testing.allocator);
    try testing.expectEqual(@as(usize, 1), harness.shutdowns);
}

test "collector restarts source after transient startup failure" {
    const testing = std.testing;

    const Mock = struct {
        const SourceCtx = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
        };

        const StreamState = struct {
            allocator: std.mem.Allocator,
            emitted: bool = false,
            event_storage: [1]event.Event,
        };

        pub var start_attempts: usize = 0;

        pub fn reset() void {
            start_attempts = 0;
        }

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const descriptor = source.SourceDescriptor{ .type = .syslog, .name = config.id };
            const state = try ctx.allocator.create(SourceCtx);
            state.* = .{ .allocator = ctx.allocator, .descriptor = descriptor };

            const lifecycle = source.StreamLifecycle{
                .start_stream = startStream,
                .shutdown = shutdown,
            };

            return source.Source{
                .stream = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = true, .batching = false },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asCtx(ptr: *anyopaque) *SourceCtx {
            const aligned: *align(@alignOf(SourceCtx)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn asStream(ptr: *anyopaque) *StreamState {
            const aligned: *align(@alignOf(StreamState)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            start_attempts += 1;
            if (start_attempts == 1) {
                return source.SourceError.StartupFailed;
            }

            const state = try allocator.create(StreamState);
            const ctx = asCtx(context);
            state.* = .{
                .allocator = allocator,
                .event_storage = .{event.Event{
                    .metadata = .{ .source_id = ctx.descriptor.name },
                    .payload = .{ .log = .{ .message = "restarted", .fields = &[_]event.Field{} } },
                }},
            };

            return event.EventStream{
                .context = state,
                .next_fn = streamNext,
                .finish_fn = streamFinish,
            };
        }

        fn streamNext(context: *anyopaque, _: std.mem.Allocator) event.StreamError!?event.EventBatch {
            const state = asStream(context);
            if (state.emitted) return event.StreamError.EndOfStream;
            state.emitted = true;
            return event.EventBatch{ .events = state.event_storage[0..1], .ack = event.AckToken.none() };
        }

        fn streamFinish(context: *anyopaque, allocator: std.mem.Allocator) void {
            const state = asStream(context);
            allocator.destroy(state);
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            const state = asCtx(context);
            allocator.destroy(state);
        }
    };

    Mock.reset();

    var clock_state = TestClock{ .now_ns = 0 };

    const config = cfg.SourceConfig{
        .id = "restart_source",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{
        .registry = registry,
        .restart = .{
            .clock = Clock{ .context = &clock_state, .now_fn = TestClock.now },
            .rng_seed = 42,
        },
    };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try collector.start(testing.allocator);
    try testing.expectEqual(@as(usize, 1), Mock.start_attempts);

    const handle = &collector.sources.items[0];
    try testing.expect(handle.restart_state.pending_restart);

    const resume_ns = handle.restart_state.backoff.resume_at_ns;
    if (resume_ns > 0) {
        clock_state.now_ns = resume_ns - 1;
        try testing.expect((try collector.poll(testing.allocator)) == null);
        try testing.expectEqual(@as(usize, 1), Mock.start_attempts);
    }

    clock_state.now_ns = resume_ns;
    const maybe_batch = try collector.poll(testing.allocator);
    try testing.expect(maybe_batch != null);
    try testing.expectEqual(@as(usize, 2), Mock.start_attempts);
    try testing.expect(!handle.restart_state.pending_restart);
    try testing.expect(transportIsStreaming(handle));
    try testing.expectEqual(@as(usize, 1), maybe_batch.?.batch.events.len);

    const drained = try collector.poll(testing.allocator);
    try testing.expect(drained == null);
}

test "collector marks permanent start failures as shutdown" {
    const testing = std.testing;

    const Mock = struct {
        fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const descriptor = source.SourceDescriptor{ .type = .syslog, .name = config.id };
            const state = try ctx.allocator.create(source.SourceDescriptor);
            state.* = descriptor;
            const lifecycle = source.StreamLifecycle{
                .start_stream = startStream,
                .shutdown = shutdown,
            };

            return source.Source{
                .stream = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = true, .batching = false },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            _ = context;
            _ = allocator;
            return source.SourceError.InvalidConfiguration;
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            const aligned: *align(@alignOf(source.SourceDescriptor)) anyopaque = @alignCast(context);
            const descriptor_ptr: *source.SourceDescriptor = @ptrCast(aligned);
            allocator.destroy(descriptor_ptr);
        }
    };

    const config = cfg.SourceConfig{
        .id = "invalid_source",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{ .registry = registry };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    try testing.expectError(CollectorError.InvalidConfiguration, collector.start(testing.allocator));
    const handle = &collector.sources.items[0];
    try testing.expect(handle.shutting_down);
    try testing.expect(!handle.restart_state.pending_restart);
    try testing.expect(!collector.started);
}

test "collector enforces restart failure budget hold" {
    const testing = std.testing;

    const Mock = struct {
        pub var attempts: usize = 0;
        pub const fail_before_success: usize = 3;

        const SourceCtx = struct {
            allocator: std.mem.Allocator,
            descriptor: source.SourceDescriptor,
        };

        pub fn reset() void {
            attempts = 0;
        }

        pub fn factory() source.SourceFactory {
            return .{ .type = .syslog, .create = create };
        }

        fn create(ctx: source.InitContext, config: *const cfg.SourceConfig) source.SourceError!source.Source {
            const descriptor = source.SourceDescriptor{ .type = .syslog, .name = config.id };
            const state = try ctx.allocator.create(SourceCtx);
            state.* = .{ .allocator = ctx.allocator, .descriptor = descriptor };

            const lifecycle = source.StreamLifecycle{
                .start_stream = startStream,
                .shutdown = shutdown,
            };

            return source.Source{
                .stream = .{
                    .descriptor = descriptor,
                    .capabilities = .{ .streaming = true, .batching = false },
                    .context = state,
                    .lifecycle = lifecycle,
                },
            };
        }

        fn asCtx(ptr: *anyopaque) *SourceCtx {
            const aligned: *align(@alignOf(SourceCtx)) anyopaque = @alignCast(ptr);
            return @ptrCast(aligned);
        }

        fn startStream(context: *anyopaque, allocator: std.mem.Allocator) source.SourceError!?event.EventStream {
            _ = allocator;
            _ = asCtx(context);
            attempts += 1;
            if (attempts <= fail_before_success) {
                return source.SourceError.StartupFailed;
            }
            return null;
        }

        fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
            const state = asCtx(context);
            allocator.destroy(state);
        }
    };

    Mock.reset();

    var clock_state = TestClock{ .now_ns = 0 };

    const config = cfg.SourceConfig{
        .id = "budget_source",
        .payload = .{ .syslog = cfg.SyslogConfig{ .address = "127.0.0.1:514" } },
    };

    const registry = source.Registry{ .factories = &[_]source.SourceFactory{Mock.factory()} };
    const options = CollectorOptions{
        .registry = registry,
        .restart = .{
            .clock = Clock{ .context = &clock_state, .now_fn = TestClock.now },
            .rng_seed = 7,
        },
    };

    var collector = try Collector.init(testing.allocator, &.{config}, options);
    defer collector.deinit();

    const custom_policy = RestartPolicy{
        .backoff = .{
            .min_ns = 1,
            .max_ns = 10,
            .multiplier_num = 2,
            .multiplier_den = 1,
            .jitter = .full,
        },
        .budget = .{
            .window_ns = 1_000,
            .limit = 2,
            .hold_ns = 5_000,
        },
    };

    collector.sources.items[0].restart_policy = custom_policy;
    const test_random_source = restart.RandomSource{
        .context = &collector,
        .next_fn = struct {
            fn call(context: ?*anyopaque) u64 {
                std.debug.assert(context != null);
                const aligned: *align(@alignOf(Collector)) anyopaque = @alignCast(context.?);
                const self: *Collector = @ptrCast(aligned);
                return self.nextRandom();
            }
        }.call,
    };
    collector.sources.items[0].restart_state = RestartState.init(test_random_source);

    try collector.start(testing.allocator);
    try testing.expectEqual(@as(usize, 1), Mock.attempts);

    const handle = &collector.sources.items[0];
    try testing.expect(handle.restart_state.pending_restart);

    const resume_ns = handle.restart_state.backoff.resume_at_ns;
    clock_state.now_ns = resume_ns;

    try testing.expect((try collector.poll(testing.allocator)) == null);
    try testing.expectEqual(@as(usize, 2), Mock.attempts);
    try testing.expect(handle.restart_state.pending_restart);
    try testing.expect(restart.isUnderQuarantine(&handle.restart_state, resume_ns));

    const expected_hold = resume_ns + custom_policy.budget.hold_ns;
    try testing.expectEqual(expected_hold, handle.restart_state.budget.hold_until_ns);

    clock_state.now_ns = expected_hold - 1;
    try testing.expect((try collector.poll(testing.allocator)) == null);
    try testing.expectEqual(@as(usize, 2), Mock.attempts);

    clock_state.now_ns = expected_hold;
    try testing.expect((try collector.poll(testing.allocator)) == null);
    try testing.expectEqual(@as(usize, 3), Mock.attempts);
}
