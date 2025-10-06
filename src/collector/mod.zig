const std = @import("std");
const source = @import("source");
const netx = @import("netx");
const cfg = source.config;
const event = source.event;
const meta = std.meta;
const math = std.math;

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

/// Customisation hooks allowing embedders to provide alternative source registry,
/// logging and metrics plumbing.
pub const CollectorOptions = struct {
    registry: source.Registry = source.Registry.builtin(),
    log: ?*const source.Logger = null,
    metrics: ?*const source.Metrics = null,
};

/// Metric identifiers emitted by the collector when metrics plumbing is enabled.
const collector_metrics = struct {
    const batches = "collector_batches_emitted_total";
    const events = "collector_events_emitted_total";
    const poll_errors = "collector_poll_errors_total";
    const backpressure = "collector_backpressure_total";
    const active_sources = "collector_active_sources";
    const ready_skips = "collector_ready_skips_total";
    const empty_polls = "collector_empty_polls_total";
};

const max_ready_skip_cycles: usize = 8;

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

/// Convenience helper for incrementing numeric metrics.
fn recordCounter(options: CollectorOptions, name: []const u8, value: u64) void {
    if (options.metrics) |metrics_sink| {
        metrics_sink.incrCounter(name, value);
    }
}

/// Carrier struct used internally to track the lifecycle of each configured source.
pub const SourceHandle = struct {
    descriptor: source.SourceDescriptor,
    instance: source.Source,
    stream: ?event.EventStream = null,
    supports_batching: bool,
    shutting_down: bool = false,
    ready_skip_count: usize = 0,
    consecutive_empty_polls: usize = 0,
};

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
    runtime: netx.runtime.IoRuntime,
    options: CollectorOptions,
    sources: std.ArrayListUnmanaged(SourceHandle) = .{},
    next_index: usize = 0,
    started: bool = false,

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
            .sources = .{},
            .next_index = 0,
            .started = false,
        };

        collector.runtime = netx.runtime.IoRuntime.initDefault() catch {
            return CollectorError.InitializationFailed;
        };

        var cleanup_needed = true;
        errdefer if (cleanup_needed) {
            destroySourceHandles(&collector, allocator);
            collector.sources.deinit(allocator);
            collector.runtime.deinit();
        };

        for (configs) |config| {
            const source_type = meta.activeTag(config.payload);
            const factory = options.registry.findFactory(source_type) orelse {
                return CollectorError.UnsupportedSource;
            };

            const init_ctx = source.InitContext{
                .allocator = allocator,
                .runtime = &collector.runtime,
                .log = options.log,
                .metrics = options.metrics,
            };

            var instance = factory.create(init_ctx, &config) catch {
                return CollectorError.InitializationFailed;
            };

            const handle = SourceHandle{
                .descriptor = instance.descriptor,
                .instance = instance,
                .stream = null,
                .supports_batching = instance.lifecycle.poll_batch != null,
                .shutting_down = false,
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
        destroySourceHandles(self, self.allocator);
        self.sources.deinit(self.allocator);
        self.runtime.deinit();
    }

    /// Starts streaming and/or batching for each configured source. Idempotent.
    pub fn start(self: *Collector, allocator: std.mem.Allocator) CollectorError!void {
        if (self.started) return;

        for (self.sources.items) |*handle| {
            if (handle.shutting_down) {
                logError(self.options, "collector start aborted: source {s} already shutdown", .{handle.descriptor.name});
                return CollectorError.StartFailed;
            }

            const capabilities = handle.instance.capabilities;
            if (!capabilities.streaming and !handle.supports_batching) {
                logError(
                    self.options,
                    "collector start aborted: source {s} exposes neither streaming nor batching",
                    .{handle.descriptor.name},
                );
                return CollectorError.InvalidConfiguration;
            }

            handle.stream = null;
            if (capabilities.streaming) {
                const maybe_stream = handle.instance.startStream(allocator) catch |err| {
                    return mapStartError(self, handle.descriptor, err);
                };

                if (maybe_stream) |stream| {
                    handle.stream = stream;
                    logInfo(self.options, "collector started stream for {s}", .{handle.descriptor.name});
                } else if (!handle.supports_batching) {
                    logError(
                        self.options,
                        "collector start aborted: source {s} did not expose stream",
                        .{handle.descriptor.name},
                    );
                    return CollectorError.StartFailed;
                } else {
                    logInfo(
                        self.options,
                        "collector using batch polling for {s}",
                        .{handle.descriptor.name},
                    );
                }
            } else if (handle.supports_batching) {
                logInfo(self.options, "collector using batch-only mode for {s}", .{handle.descriptor.name});
            }
        }

        self.started = true;
        self.next_index = 0;
        updateActiveSourcesGauge(self);
        logInfo(self.options, "collector started ({d} sources)", .{self.sources.items.len});
    }

    /// Returns a snapshot that can be emitted via logs/metrics to describe the
    /// collector state.
    pub fn status(self: *const Collector) Status {
        var active: usize = 0;
        var streaming: usize = 0;

        for (self.sources.items) |handle| {
            if (!handle.shutting_down) active += 1;
            if (handle.stream != null) streaming += 1;
        }

        return Status{
            .total_sources = self.sources.items.len,
            .active_sources = active,
            .streaming_sources = streaming,
            .started = self.started,
        };
    }

    /// Attempts to fetch the next available batch from any active source. The
    /// round-robin schedule is preserved across calls.
    pub fn poll(self: *Collector, allocator: std.mem.Allocator) CollectorError!?PolledBatch {
        if (!self.started) return CollectorError.NotStarted;

        const total = self.sources.items.len;
        if (total == 0) return null;

        var index = if (self.next_index < total) self.next_index else 0;
        var examined: usize = 0;

        while (examined < total) : (examined += 1) {
            var handle = &self.sources.items[index];

            if (handle.shutting_down) {
                index = (index + 1) % total;
                continue;
            }

            const ready = handle.instance.readyHint();
            const forced_poll = !ready and handle.ready_skip_count >= max_ready_skip_cycles;
            if (!ready and !forced_poll) {
                if (handle.ready_skip_count < math.maxInt(usize)) {
                    handle.ready_skip_count += 1;
                }
                recordReadySkip(self);
                index = (index + 1) % total;
                continue;
            }
            if (forced_poll) {
                logInfo(
                    self.options,
                    "collector forcing poll for {s} after {d} ready skips",
                    .{ handle.descriptor.name, handle.ready_skip_count },
                );
            }
            handle.ready_skip_count = 0;

            const result = try self.tryPoll(handle, allocator);

            switch (result.status) {
                .yielded => {
                    const batch = result.batch orelse unreachable;
                    handle.consecutive_empty_polls = 0;
                    recordBatchMetrics(self, batch.events.len);
                    self.next_index = (index + 1) % total;
                    return PolledBatch{
                        .descriptor = handle.descriptor,
                        .batch = batch,
                    };
                },
                .backpressure => return CollectorError.Backpressure,
                else => {},
            }

            if (result.attempted and result.status != .yielded and result.status != .backpressure) {
                if (handle.consecutive_empty_polls < math.maxInt(usize)) {
                    handle.consecutive_empty_polls += 1;
                }
                recordEmptyPoll(self);
            }

            index = (index + 1) % total;
        }

        self.next_index = index;
        return null;
    }

    fn tryPoll(self: *Collector, handle: *SourceHandle, allocator: std.mem.Allocator) CollectorError!PollResult {
        if (handle.stream) |*stream_handle| {
            const batch = stream_handle.next(allocator) catch |err| switch (err) {
                error.EndOfStream => {
                    stream_handle.finish(allocator);
                    handle.stream = null;
                    logInfo(self.options, "collector stream finished for {s}", .{handle.descriptor.name});
                    return PollResult{ .status = .finished, .batch = null, .attempted = true };
                },
                error.Backpressure => {
                    recordBackpressure(self);
                    logWarn(
                        self.options,
                        "collector stream backpressure for {s}",
                        .{handle.descriptor.name},
                    );
                    return PollResult{ .status = .backpressure, .batch = null, .attempted = true };
                },
                error.TransportFailure, error.DecodeFailure => {
                    recordPollError(self);
                    logError(
                        self.options,
                        "collector stream failure for {s}: {s}",
                        .{ handle.descriptor.name, @errorName(err) },
                    );
                    return CollectorError.PollFailed;
                },
            };

            if (batch) |value| {
                return PollResult{ .status = .yielded, .batch = value, .attempted = true };
            }

            return PollResult{ .status = .empty, .batch = null, .attempted = true };
        }

        if (handle.supports_batching) {
            const batch = handle.instance.pollBatch(allocator) catch |err| switch (err) {
                source.SourceError.Backpressure => {
                    recordBackpressure(self);
                    logWarn(
                        self.options,
                        "collector batch backpressure for {s}",
                        .{handle.descriptor.name},
                    );
                    return PollResult{ .status = .backpressure, .batch = null, .attempted = true };
                },
                source.SourceError.OperationNotSupported => {
                    logError(
                        self.options,
                        "collector invalid configuration: batch polling unsupported for {s}",
                        .{handle.descriptor.name},
                    );
                    return CollectorError.InvalidConfiguration;
                },
                else => {
                    recordPollError(self);
                    logError(
                        self.options,
                        "collector batch failure for {s}: {s}",
                        .{ handle.descriptor.name, @errorName(err) },
                    );
                    return CollectorError.PollFailed;
                },
            };

            if (batch) |value| {
                return PollResult{ .status = .yielded, .batch = value, .attempted = true };
            }

            return PollResult{ .status = .empty, .batch = null, .attempted = true };
        }

        return PollResult{ .status = .idle, .batch = null, .attempted = false };
    }

    /// Shuts down all sources and resets collector bookkeeping.
    pub fn shutdown(self: *Collector, allocator: std.mem.Allocator) CollectorError!void {
        destroySourceHandles(self, allocator);
        self.started = false;
        self.next_index = 0;
        updateActiveSourcesGauge(self);
        logInfo(self.options, "collector shutdown complete", .{});
    }
};

fn destroySourceHandles(collector: *Collector, allocator: std.mem.Allocator) void {
    if (collector.sources.items.len == 0) return;

    for (collector.sources.items) |*handle| {
        if (handle.shutting_down) continue;

        if (handle.stream) |*stream_handle| {
            stream_handle.finish(allocator);
            handle.stream = null;
        }

        handle.instance.shutdown(allocator);
        handle.shutting_down = true;
    }
}

fn mapStartError(self: *Collector, descriptor: source.SourceDescriptor, err: source.SourceError) CollectorError {
    switch (err) {
        error.Backpressure => {
            recordBackpressure(self);
            logWarn(self.options, "collector start backpressure for {s}", .{descriptor.name});
            return CollectorError.Backpressure;
        },
        else => {
            logError(
                self.options,
                "collector start failed for {s}: {s}",
                .{ descriptor.name, @errorName(err) },
            );
            return CollectorError.StartFailed;
        },
    }
}

fn updateActiveSourcesGauge(self: *Collector) void {
    if (self.options.metrics) |metrics_sink| {
        var active: usize = 0;
        for (self.sources.items) |handle| {
            if (!handle.shutting_down) active += 1;
        }

        if (math.cast(i64, active)) |value| {
            metrics_sink.recordGauge(collector_metrics.active_sources, value);
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

fn recordReadySkip(self: *Collector) void {
    recordCounter(self.options, collector_metrics.ready_skips, 1);
}

fn recordEmptyPoll(self: *Collector) void {
    recordCounter(self.options, collector_metrics.empty_polls, 1);
}

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

            return source.Source{
                .descriptor = descriptor,
                .capabilities = .{ .streaming = false, .batching = true },
                .lifecycle = .{
                    .start_stream = startStream,
                    .poll_batch = pollBatch,
                    .shutdown = shutdown,
                    .ready_hint = readyHint,
                },
                .context = state,
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

            return source.Source{
                .descriptor = descriptor,
                .capabilities = .{ .streaming = false, .batching = true },
                .lifecycle = .{
                    .start_stream = startStream,
                    .poll_batch = pollBatch,
                    .shutdown = shutdown,
                    .ready_hint = readyHint,
                },
                .context = state,
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

            return source.Source{
                .descriptor = descriptor,
                .capabilities = .{ .streaming = false, .batching = true },
                .lifecycle = .{
                    .start_stream = startStream,
                    .poll_batch = pollBatch,
                    .shutdown = shutdown,
                    .ready_hint = readyHint,
                },
                .context = state,
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

            return source.Source{
                .descriptor = descriptor,
                .capabilities = .{ .streaming = true, .batching = false },
                .lifecycle = .{
                    .start_stream = startStream,
                    .poll_batch = null,
                    .shutdown = shutdown,
                    .ready_hint = readyHint,
                },
                .context = state,
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

            return source.Source{
                .descriptor = descriptor,
                .capabilities = .{ .streaming = true, .batching = false },
                .lifecycle = .{
                    .start_stream = startStream,
                    .poll_batch = null,
                    .shutdown = shutdown,
                    .ready_hint = readyHint,
                },
                .context = state,
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
