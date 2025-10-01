const std = @import("std");
const src = @import("source.zig");
const cfg = @import("config.zig");
const event = @import("event.zig");
const buffer = @import("buffer.zig");
const netx = @import("netx");
const netx_transport = netx.transport;
const netx_options = netx.options;
const netx_udp = netx.udp;
const netx_tcp = netx.tcp;
const netx_runtime = netx.runtime;
const parser = @import("syslog/parser.zig");
const frame = @import("syslog/frame.zig");

const TransportManager = struct {
    pub const Error = error{
        StartFailure,
        PollFailure,
    };

    runtime: *netx_runtime.IoRuntime,
    transport: ?netx_transport.Transport = null,

    fn init(
        ctx: src.InitContext,
        descriptor: src.SourceDescriptor,
        config: cfg.SyslogConfig,
    ) Error!TransportManager {
        _ = descriptor;

        var manager = TransportManager{
            .runtime = ctx.runtime,
            .transport = null,
        };

        switch (config.transport) {
            .udp => {
                const transport_instance = createUdpTransport(ctx.allocator, ctx.runtime, config) catch {
                    return Error.StartFailure;
                };
                manager.transport = transport_instance;
            },
            .tcp => {
                const transport_instance = createTcpTransport(ctx.allocator, ctx.runtime, config) catch {
                    return Error.StartFailure;
                };
                manager.transport = transport_instance;
            },
        }

        return manager;
    }

    fn start(self: *TransportManager, state: *SyslogState) Error!void {
        _ = state;
        if (self.transport) |*t| {
            t.start() catch {
                return Error.StartFailure;
            };
        }
    }

    fn poll(self: *TransportManager, allocator: std.mem.Allocator) Error!?netx_transport.Message {
        self.runtime.tick() catch {
            return Error.PollFailure;
        };

        if (self.transport) |*t| {
            return t.poll(allocator) catch {
                return Error.PollFailure;
            };
        }
        return null;
    }

    fn shutdown(self: *TransportManager, state: *SyslogState) void {
        _ = state;
        if (self.transport) |*t| {
            t.shutdown();
            self.transport = null;
        }
    }
};

fn createUdpTransport(
    allocator: std.mem.Allocator,
    runtime: *netx_runtime.IoRuntime,
    config: cfg.SyslogConfig,
) netx_transport.TransportError!netx_transport.Transport {
    const limits = netx_options.Limits{
        .read_buffer_bytes = config.read_buffer_bytes,
        .message_size_limit = config.message_size_limit,
    };

    const udp_opts = netx_options.UdpOptions{
        .address = config.address,
        .limits = limits,
    };

    return netx_udp.create(allocator, runtime.loopHandle(), udp_opts);
}

fn createTcpTransport(
    allocator: std.mem.Allocator,
    runtime: *netx_runtime.IoRuntime,
    config: cfg.SyslogConfig,
) netx_transport.TransportError!netx_transport.Transport {
    const limits = netx_options.Limits{
        .read_buffer_bytes = config.read_buffer_bytes,
        .message_size_limit = config.message_size_limit,
    };

    const tcp_opts = netx_options.TcpOptions{
        .address = config.address,
        .limits = limits,
        .max_connections = config.tcp_max_connections,
        .keepalive_seconds = config.tcp_keepalive_seconds,
        .high_watermark = config.tcp_high_watermark,
        .low_watermark = config.tcp_low_watermark,
    };

    return netx_tcp.create(allocator, runtime.loopHandle(), tcp_opts);
}

/// Skeleton implementation of a syslog source. The final version will ingest
/// messages via UDP/TCP sockets, decode RFC3164/RFC5424 payloads, and expose
/// both streaming and batched delivery modes.
pub fn factory() src.SourceFactory {
    return .{
        .type = .syslog,
        .create = create,
    };
}

const EventBuffer = buffer.BoundedBuffer(event.ManagedEvent);

const SyslogState = struct {
    allocator: std.mem.Allocator,
    descriptor: src.SourceDescriptor,
    config: cfg.SyslogConfig,
    buffer: EventBuffer,
    log: ?*const src.Logger,
    metrics: ?*const src.Metrics,
    stream_closed: bool = false,
    transport: TransportManager,
    framer: frame.Framer,
};

const BatchAckContext = struct {
    state_allocator: std.mem.Allocator,
    batch_allocator: std.mem.Allocator,
    allocated: []event.Event,
    finalizers: []event.EventFinalizer,
    view_len: usize,
    metrics: ?*const src.Metrics,
    log: ?*const src.Logger,
    descriptor_name: []const u8,
    handle: event.AckHandle,
};

const DrainError = error{
    Backpressure,
};

const metrics = struct {
    const enqueued = "sources_syslog_events_enqueued_total";
    const dropped = "sources_syslog_events_dropped_total";
    const rejected = "sources_syslog_events_rejected_total";
    const emitted = "sources_syslog_events_emitted_total";
    const truncated = "sources_syslog_events_truncated_total";
    const queue_depth = "sources_syslog_buffer_depth";
    const ack_success = "sources_syslog_batch_ack_success_total";
    const ack_retryable = "sources_syslog_batch_ack_retryable_total";
    const ack_failure = "sources_syslog_batch_ack_failure_total";
};

fn batchAckComplete(context: *anyopaque, status: event.AckStatus) void {
    const aligned: *align(@alignOf(BatchAckContext)) anyopaque = @alignCast(context);
    const ack: *BatchAckContext = @ptrCast(aligned);
    defer {
        ack.state_allocator.free(ack.finalizers);
        ack.batch_allocator.free(ack.allocated);
        ack.state_allocator.destroy(ack);
    }

    var index: usize = 0;
    while (index < ack.view_len) : (index += 1) {
        ack.finalizers[index].run();
    }

    if (ack.metrics) |metrics_sink| {
        const counter_name = switch (status) {
            .success => metrics.ack_success,
            .retryable_failure => metrics.ack_retryable,
            .permanent_failure => metrics.ack_failure,
        };
        metrics_sink.incrCounter(counter_name, 1);
    }

    if (ack.log) |logger| {
        switch (status) {
            .success => {},
            .retryable_failure => logger.warnf(
                "syslog source {s}: downstream reported retryable failure",
                .{ack.descriptor_name},
            ),
            .permanent_failure => logger.errorf(
                "syslog source {s}: downstream reported permanent failure",
                .{ack.descriptor_name},
            ),
        }
    }
}

fn create(ctx: src.InitContext, config: *const cfg.SourceConfig) src.SourceError!src.Source {
    const syslog_cfg = switch (config.payload) {
        .syslog => |value| value,
    };

    const descriptor = src.SourceDescriptor{
        .type = .syslog,
        .name = config.id,
    };

    const state = ctx.allocator.create(SyslogState) catch return src.SourceError.StartupFailed;
    errdefer ctx.allocator.destroy(state);

    const transport_manager = TransportManager.init(ctx, descriptor, syslog_cfg) catch
        return src.SourceError.StartupFailed;

    state.* = .{
        .allocator = ctx.allocator,
        .descriptor = descriptor,
        .config = syslog_cfg,
        .buffer = undefined,
        .log = ctx.log,
        .metrics = ctx.metrics,
        .transport = transport_manager,
        .framer = frame.Framer.init(ctx.allocator, .auto, syslog_cfg.message_size_limit),
    };
    errdefer state.framer.deinit();

    const buffer_instance = EventBuffer.init(ctx.allocator, syslog_cfg.max_batch_size, .reject) catch
        return src.SourceError.StartupFailed;
    state.buffer = buffer_instance;
    errdefer state.buffer.deinit();

    state.transport.start(state) catch |err| {
        state.buffer.deinit();
        state.allocator.destroy(state);
        logError(state, "syslog source {s}: failed to start transport", .{descriptor.name});
        return switch (err) {
            TransportManager.Error.StartFailure => src.SourceError.StartupFailed,
            TransportManager.Error.PollFailure => src.SourceError.StartupFailed,
        };
    };

    return src.Source{
        .descriptor = descriptor,
        .capabilities = .{ .streaming = true, .batching = true },
        .lifecycle = .{
            .start_stream = startStream,
            .poll_batch = pollBatch,
            .shutdown = shutdown,
            .ready_hint = readyHint,
        },
        .context = state,
    };
}

fn startStream(context: *anyopaque, allocator: std.mem.Allocator) src.SourceError!?event.EventStream {
    _ = allocator;
    const state = asState(context);
    state.stream_closed = false;

    return event.EventStream{
        .context = state,
        .next_fn = streamNext,
        .finish_fn = streamFinish,
    };
}

fn streamNext(context: *anyopaque, allocator: std.mem.Allocator) event.StreamError!?event.EventBatch {
    const state = asState(context);
    if (state.stream_closed) return event.StreamError.EndOfStream;

    pumpTransport(state, allocator);

    const result = drainBatch(state, allocator) catch {
        return event.StreamError.Backpressure;
    };
    return result;
}

fn streamFinish(context: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const state = asState(context);
    state.stream_closed = true;
}

fn pollBatch(context: *anyopaque, allocator: std.mem.Allocator) src.SourceError!?event.EventBatch {
    const state = asState(context);
    pumpTransport(state, allocator);
    const result = drainBatch(state, allocator) catch {
        return src.SourceError.Backpressure;
    };
    return result;
}

fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const state = asState(context);
    state.transport.shutdown(state);
    state.buffer.deinit();
    state.framer.deinit();
    state.allocator.destroy(state);
}

fn readyHint(context: *anyopaque) bool {
    const state = asState(context);
    if (!state.buffer.isEmpty()) return true;
    return state.framer.hasBufferedData();
}

fn asState(ptr: *anyopaque) *SyslogState {
    const aligned: *align(@alignOf(SyslogState)) anyopaque = @alignCast(ptr);
    return @ptrCast(aligned);
}

fn enqueueEvent(state: *SyslogState, managed: event.ManagedEvent) src.SourceError!void {
    const result = state.buffer.push(managed) catch {
        managed.finalizer.run();
        recordReject(state, 1);
        logWarn(
            state,
            "syslog source {s}: buffer is full (reject policy)",
            .{state.descriptor.name},
        );
        return src.SourceError.Backpressure;
    };

    switch (result) {
        .stored => recordEnqueued(state, 1),
        .dropped_newest => {
            recordDrop(state, 1);
            managed.finalizer.run();
            logWarn(
                state,
                "syslog source {s}: drop_newest policy dropped newest event",
                .{state.descriptor.name},
            );
        },
        .dropped_oldest => |evicted| {
            recordDrop(state, 1);
            evicted.finalizer.run();
            recordEnqueued(state, 1);
            logWarn(
                state,
                "syslog source {s}: drop_oldest policy evicted oldest event",
                .{state.descriptor.name},
            );
        },
    }

    recordQueueDepth(state);
}

fn pumpTransport(state: *SyslogState, allocator: std.mem.Allocator) void {
    while (true) {
        const maybe_message = state.transport.poll(allocator) catch |err| {
            logError(state, "syslog source {s}: transport poll failed ({s})", .{ state.descriptor.name, @errorName(err) });
            return;
        };
        const message = maybe_message orelse break;

        const chunk_finalizer = message.finalizer;

        state.framer.push(message.bytes) catch {
            if (chunk_finalizer) |finalizer| finalizer.run();
            recordReject(state, 1);
            logError(state, "syslog source {s}: failed to buffer tcp chunk", .{state.descriptor.name});
            continue;
        };

        while (true) {
            const extracted = state.framer.next() catch |err| {
                recordReject(state, 1);
                logWarn(state, "syslog source {s}: dropped invalid frame ({s})", .{ state.descriptor.name, @errorName(err) });
                continue;
            };

            const frame_result = extracted orelse break;

            const peer_copy = state.allocator.alloc(u8, message.metadata.peer_address.len) catch {
                state.allocator.free(frame_result.payload);
                recordReject(state, 1);
                logError(state, "syslog source {s}: failed to copy peer address", .{state.descriptor.name});
                continue;
            };
            if (message.metadata.peer_address.len != 0) {
                @memcpy(peer_copy, message.metadata.peer_address);
            }

            const allocation = netx_transport.IoAllocation.create(state.allocator, frame_result.payload, peer_copy) catch {
                state.allocator.free(frame_result.payload);
                state.allocator.free(peer_copy);
                recordReject(state, 1);
                logError(state, "syslog source {s}: failed to allocate frame wrappers", .{state.descriptor.name});
                continue;
            };

            const framed_message = netx_transport.Message{
                .bytes = allocation.payload,
                .metadata = .{
                    .protocol = message.metadata.protocol,
                    .peer_address = allocation.peer,
                    .truncated = frame_result.truncated,
                },
                .finalizer = netx_transport.IoAllocation.finalizer(allocation),
            };

            if (framed_message.metadata.truncated) {
                recordTruncated(state, 1);
            }

            const parsed = parser.parseMessage(allocator, state.config, framed_message) catch |err| {
                if (framed_message.finalizer) |finalizer| {
                    finalizer.run();
                }
                recordReject(state, 1);
                logWarn(state, "syslog source {s}: failed to parse message ({s})", .{ state.descriptor.name, @errorName(err) });
                continue;
            };

            enqueueEvent(state, parsed.managed) catch |err| {
                switch (err) {
                    src.SourceError.Backpressure => {},
                    else => logError(state, "syslog source {s}: failed to enqueue parsed event ({s})", .{ state.descriptor.name, @errorName(err) }),
                }
                continue;
            };
        }

        if (chunk_finalizer) |finalizer| finalizer.run();
    }
}

fn drainBatch(state: *SyslogState, allocator: std.mem.Allocator) DrainError!?event.EventBatch {
    if (state.buffer.isEmpty()) return null;

    const to_take = @min(state.config.max_batch_size, state.buffer.len());
    if (to_take == 0) return null;

    const ack_context = state.allocator.create(BatchAckContext) catch
        return DrainError.Backpressure;

    const allocated = allocator.alloc(event.Event, to_take) catch {
        logError(state, "syslog source {s}: failed to allocate batch slice", .{state.descriptor.name});
        state.allocator.destroy(ack_context);
        return DrainError.Backpressure;
    };

    const finalizers = state.allocator.alloc(event.EventFinalizer, to_take) catch {
        allocator.free(allocated);
        state.allocator.destroy(ack_context);
        return DrainError.Backpressure;
    };

    var count: usize = 0;
    while (count < to_take) : (count += 1) {
        const maybe_event = state.buffer.pop() orelse break;
        allocated[count] = maybe_event.event;
        finalizers[count] = maybe_event.finalizer;
    }

    if (count == 0) {
        allocator.free(allocated);
        state.allocator.free(finalizers);
        state.allocator.destroy(ack_context);
        return null;
    }

    ack_context.* = .{
        .state_allocator = state.allocator,
        .batch_allocator = allocator,
        .allocated = allocated,
        .finalizers = finalizers,
        .view_len = count,
        .metrics = state.metrics,
        .log = state.log,
        .descriptor_name = state.descriptor.name,
        .handle = undefined,
    };
    ack_context.handle = event.AckHandle.init(ack_context, batchAckComplete);

    recordEmitted(state, count);
    recordQueueDepth(state);

    return event.EventBatch{
        .events = ack_context.allocated[0..ack_context.view_len],
        .ack = event.AckToken.init(&ack_context.handle),
    };
}

fn recordEnqueued(state: *SyslogState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.enqueued, delta);
    }
}

fn recordDrop(state: *SyslogState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.dropped, delta);
    }
}

fn recordReject(state: *SyslogState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.rejected, delta);
    }
}

fn recordTruncated(state: *SyslogState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.truncated, delta);
    }
}

fn recordEmitted(state: *SyslogState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.emitted, delta);
    }
}

fn recordQueueDepth(state: *SyslogState) void {
    if (state.metrics) |metrics_sink| {
        const depth: i64 = @intCast(state.buffer.len());
        metrics_sink.recordGauge(metrics.queue_depth, depth);
    }
}

fn logWarn(state: *SyslogState, comptime fmt: []const u8, args: anytype) void {
    if (state.log) |logger| {
        logger.warnf(fmt, args);
    }
}

fn logError(state: *SyslogState, comptime fmt: []const u8, args: anytype) void {
    if (state.log) |logger| {
        logger.errorf(fmt, args);
    }
}

test "syslog pollBatch drains buffer" {
    const allocator = std.testing.allocator;
    var runtime = try netx_runtime.IoRuntime.initDefault();
    defer runtime.deinit();
    const ctx = src.InitContext{
        .allocator = allocator,
        .runtime = &runtime,
        .log = null,
        .metrics = null,
    };

    var source_config = cfg.SourceConfig{
        .id = "syslog_test",
        .payload = .{ .syslog = .{ .address = "127.0.0.1:5514" } },
    };

    var source_instance = try create(ctx, &source_config);
    defer source_instance.shutdown(allocator);

    const state = asState(source_instance.context);
    const sample_event = event.Event{
        .metadata = .{},
        .payload = .{ .log = .{ .message = "hello", .fields = &[_]event.Field{} } },
    };

    try enqueueEvent(state, event.ManagedEvent.fromEvent(sample_event));

    const maybe_batch = try source_instance.pollBatch(std.testing.allocator);
    try std.testing.expect(maybe_batch != null);
    var batch = maybe_batch.?;
    try std.testing.expectEqual(@as(usize, 1), batch.events.len);
    try std.testing.expectEqualStrings("hello", batch.events[0].payload.log.message);
    try std.testing.expect(batch.ack.isAvailable());
    try batch.ack.success();

    const next_batch = try source_instance.pollBatch(std.testing.allocator);
    try std.testing.expect(next_batch == null);
}

test "syslog ack runs managed event finalizers" {
    const allocator = std.testing.allocator;
    var runtime = try netx_runtime.IoRuntime.initDefault();
    defer runtime.deinit();
    const ctx = src.InitContext{
        .allocator = allocator,
        .runtime = &runtime,
        .log = null,
        .metrics = null,
    };

    var source_config = cfg.SourceConfig{
        .id = "syslog_finalizer",
        .payload = .{ .syslog = .{ .address = "127.0.0.1:5514" } },
    };

    var source_instance = try create(ctx, &source_config);
    defer source_instance.shutdown(allocator);

    const state = asState(source_instance.context);

    var finalized: u8 = 0;
    const Finalizer = struct {
        fn run(context: ?*anyopaque) void {
            if (context) |ptr| {
                const flag_ptr: *u8 = @ptrCast(ptr);
                flag_ptr.* = 1;
            }
        }
    };

    const finalizer = event.EventFinalizer.init(Finalizer.run, @as(?*anyopaque, @ptrCast(&finalized)));

    const managed = event.ManagedEvent.init(
        event.Event{
            .metadata = .{},
            .payload = .{ .log = .{ .message = "needs-finalizer", .fields = &[_]event.Field{} } },
        },
        finalizer,
    );

    try enqueueEvent(state, managed);

    const maybe_batch = try source_instance.pollBatch(std.testing.allocator);
    try std.testing.expect(maybe_batch != null);
    var batch = maybe_batch.?;
    try std.testing.expectEqual(@as(usize, 1), batch.events.len);
    try batch.ack.success();
    try std.testing.expectEqual(@as(u8, 1), finalized);
}

test "syslog enqueue differentiates reject and drop metrics" {
    const allocator = std.testing.allocator;

    const MetricsHarness = struct {
        drops: usize = 0,
        rejects: usize = 0,
        enqueued: usize = 0,
        last_gauge: i64 = -1,

        fn incr(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (std.mem.eql(u8, name, metrics.dropped)) {
                self.drops += value;
            } else if (std.mem.eql(u8, name, metrics.rejected)) {
                self.rejects += value;
            } else if (std.mem.eql(u8, name, metrics.enqueued)) {
                self.enqueued += value;
            }
        }

        fn gauge(context: *anyopaque, name: []const u8, value: i64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (std.mem.eql(u8, name, metrics.queue_depth)) {
                self.last_gauge = value;
            }
        }
    };

    var harness = MetricsHarness{};
    const log: ?*const src.Logger = null;
    const metrics_obj = src.Metrics{
        .context = @ptrCast(&harness),
        .incr_counter_fn = MetricsHarness.incr,
        .record_gauge_fn = MetricsHarness.gauge,
    };

    var runtime = try netx_runtime.IoRuntime.initDefault();
    defer runtime.deinit();
    const ctx = src.InitContext{
        .allocator = allocator,
        .runtime = &runtime,
        .log = log,
        .metrics = &metrics_obj,
    };

    var source_config = cfg.SourceConfig{
        .id = "syslog_metrics",
        .payload = .{ .syslog = .{
            .address = "127.0.0.1:5514",
            .max_batch_size = 1,
        } },
    };

    var source_instance = try create(ctx, &source_config);
    defer source_instance.shutdown(allocator);

    const state = asState(source_instance.context);

    var evicted_finalizer_calls: u8 = 0;
    const Finalizer = struct {
        fn bump(context: ?*anyopaque) void {
            if (context) |ptr| {
                const counter: *u8 = @ptrCast(ptr);
                counter.* += 1;
            }
        }
    };

    const initial = event.ManagedEvent.init(
        event.Event{
            .metadata = .{},
            .payload = .{ .log = .{ .message = "initial", .fields = &[_]event.Field{} } },
        },
        event.EventFinalizer.init(Finalizer.bump, @as(?*anyopaque, @ptrCast(&evicted_finalizer_calls))),
    );

    try enqueueEvent(state, initial);
    try std.testing.expectEqual(@as(usize, 1), harness.enqueued);
    try std.testing.expectEqual(@as(i64, 1), harness.last_gauge);

    var reject_finalizer_calls: u8 = 0;
    const reject_event = event.ManagedEvent.init(
        event.Event{
            .metadata = .{},
            .payload = .{ .log = .{ .message = "reject", .fields = &[_]event.Field{} } },
        },
        event.EventFinalizer.init(Finalizer.bump, @as(?*anyopaque, @ptrCast(&reject_finalizer_calls))),
    );

    try std.testing.expectError(src.SourceError.Backpressure, enqueueEvent(state, reject_event));
    try std.testing.expectEqual(@as(usize, 1), harness.rejects);
    try std.testing.expectEqual(@as(usize, 0), harness.drops);
    try std.testing.expectEqual(@as(u8, 1), reject_finalizer_calls);

    state.buffer.when_full = .drop_newest;

    var drop_newest_finalizer_calls: u8 = 0;
    const newest_event = event.ManagedEvent.init(
        event.Event{
            .metadata = .{},
            .payload = .{ .log = .{ .message = "drop-newest", .fields = &[_]event.Field{} } },
        },
        event.EventFinalizer.init(Finalizer.bump, @as(?*anyopaque, @ptrCast(&drop_newest_finalizer_calls))),
    );

    try enqueueEvent(state, newest_event);
    try std.testing.expectEqual(@as(usize, 1), harness.drops);
    try std.testing.expectEqual(@as(usize, 1), harness.rejects);
    try std.testing.expectEqual(@as(u8, 1), drop_newest_finalizer_calls);
    try std.testing.expectEqual(@as(i64, 1), harness.last_gauge);

    state.buffer.when_full = .drop_oldest;

    var new_event_finalizer_calls: u8 = 0;
    const replacement = event.ManagedEvent.init(
        event.Event{
            .metadata = .{},
            .payload = .{ .log = .{ .message = "replacement", .fields = &[_]event.Field{} } },
        },
        event.EventFinalizer.init(Finalizer.bump, @as(?*anyopaque, @ptrCast(&new_event_finalizer_calls))),
    );

    try enqueueEvent(state, replacement);
    try std.testing.expectEqual(@as(usize, 2), harness.enqueued);
    try std.testing.expectEqual(@as(usize, 2), harness.drops);
    try std.testing.expectEqual(@as(u8, 1), evicted_finalizer_calls);
    try std.testing.expectEqual(@as(u8, 0), new_event_finalizer_calls);
    try std.testing.expectEqual(@as(i64, 1), harness.last_gauge);
}

test "syslog startStream reuses batch draining semantics" {
    const allocator = std.testing.allocator;
    var runtime = try netx_runtime.IoRuntime.initDefault();
    defer runtime.deinit();
    const ctx = src.InitContext{
        .allocator = allocator,
        .runtime = &runtime,
        .log = null,
        .metrics = null,
    };

    var source_config = cfg.SourceConfig{
        .id = "syslog_stream",
        .payload = .{ .syslog = .{ .address = "127.0.0.1:5514" } },
    };

    var source_instance = try create(ctx, &source_config);
    defer source_instance.shutdown(allocator);

    const state = asState(source_instance.context);

    const maybe_stream = try source_instance.startStream(allocator);
    try std.testing.expect(maybe_stream != null);
    var stream = maybe_stream.?;

    const first = try stream.next(allocator);
    try std.testing.expect(first == null);

    try enqueueEvent(state, event.ManagedEvent.fromEvent(event.Event{
        .metadata = .{},
        .payload = .{ .log = .{ .message = "streamed", .fields = &[_]event.Field{} } },
    }));

    const maybe_batch = try stream.next(allocator);
    try std.testing.expect(maybe_batch != null);
    var batch = maybe_batch.?;
    try std.testing.expectEqual(@as(usize, 1), batch.events.len);
    try std.testing.expectEqualStrings("streamed", batch.events[0].payload.log.message);
    try batch.ack.success();

    const empty_again = try stream.next(allocator);
    try std.testing.expect(empty_again == null);

    stream.finish(allocator);
    try std.testing.expectError(event.StreamError.EndOfStream, stream.next(allocator));
}

fn testFindField(fields: []const event.Field, name: []const u8) ?event.Field {
    for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) return field;
    }
    return null;
}

test "parser handles rfc5424 message" {
    const allocator = std.testing.allocator;
    const config = cfg.SyslogConfig{
        .address = "127.0.0.1:5514",
        .transport = .udp,
        .parser = .rfc5424,
    };

    const message = netx_transport.Message{
        .bytes = "<34>1 2023-10-10T12:30:45Z host app 123 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] Test message",
        .metadata = .{},
        .finalizer = null,
    };

    var result = try parser.parseMessage(allocator, config, message);
    defer result.managed.finalizer.run();

    const log = result.managed.event.payload.log;
    try std.testing.expectEqualStrings("Test message", log.message);

    const metadata = result.managed.event.metadata;
    try std.testing.expect(!metadata.payload_truncated);
    try std.testing.expect(metadata.transport == null);

    const facility_field = testFindField(log.fields, "syslog_facility") orelse return std.testing.expect(false);
    try std.testing.expectEqual(@as(i64, 4), facility_field.value.integer);

    const severity_field = testFindField(log.fields, "syslog_severity") orelse return std.testing.expect(false);
    try std.testing.expectEqual(@as(i64, 2), severity_field.value.integer);

    const structured_field = testFindField(log.fields, "syslog_structured_data") orelse return std.testing.expect(false);
    try std.testing.expect(std.mem.indexOf(u8, structured_field.value.string, "eventID=\"1011\"") != null);
}

test "parser auto parses rfc3164" {
    const allocator = std.testing.allocator;
    const config = cfg.SyslogConfig{
        .address = "127.0.0.1:5514",
        .transport = .udp,
        .parser = .auto,
    };

    const message = netx_transport.Message{
        .bytes = "<13>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8",
        .metadata = .{},
        .finalizer = null,
    };

    var result = try parser.parseMessage(allocator, config, message);
    defer result.managed.finalizer.run();

    const log = result.managed.event.payload.log;
    try std.testing.expectEqualStrings("'su root' failed for lonvick on /dev/pts/8", log.message);

    const metadata = result.managed.event.metadata;
    try std.testing.expect(!metadata.payload_truncated);
    try std.testing.expect(metadata.transport == null);

    const app_field = testFindField(log.fields, "syslog_app_name") orelse return std.testing.expect(false);
    try std.testing.expectEqualStrings("su", app_field.value.string);

    const host_field = testFindField(log.fields, "syslog_hostname") orelse return std.testing.expect(false);
    try std.testing.expectEqualStrings("mymachine", host_field.value.string);
}

test "pumpTransport propagates truncation metadata and metrics" {
    const allocator = std.testing.allocator;
    var runtime = try netx_runtime.IoRuntime.initDefault();
    defer runtime.deinit();

    const MetricsHarness = struct {
        truncated: usize = 0,

        fn incr(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (std.mem.eql(u8, name, metrics.truncated)) {
                self.truncated += value;
            }
        }

        fn gauge(_: *anyopaque, _: []const u8, _: i64) void {}
    };

    var harness = MetricsHarness{};
    const metrics_obj = src.Metrics{
        .context = @ptrCast(&harness),
        .incr_counter_fn = MetricsHarness.incr,
        .record_gauge_fn = MetricsHarness.gauge,
    };

    const config = cfg.SyslogConfig{
        .address = "127.0.0.1:5514",
        .max_batch_size = 4,
    };

    const FakeTransport = struct {
        message: ?netx_transport.Message,

        fn get(context: *anyopaque) *@This() {
            const aligned: *align(@alignOf(@This())) anyopaque = @alignCast(context);
            return @ptrCast(aligned);
        }

        fn start(_: *anyopaque) netx_transport.TransportError!void {
            return;
        }

        fn poll(context: *anyopaque, arena: std.mem.Allocator) netx_transport.TransportError!?netx_transport.Message {
            _ = arena;
            const self = get(context);
            const payload = self.message orelse return null;
            self.message = null;
            return payload;
        }

        fn shutdown(context: *anyopaque) void {
            const self = get(context);
            self.message = null;
        }
    };

    const message = netx_transport.Message{
        .bytes = "<13>Oct 11 22:14:15 mymachine app: truncated payload",
        .metadata = .{
            .peer_address = "10.0.0.1:5514",
            .protocol = "tcp",
            .truncated = true,
        },
        .finalizer = null,
    };

    var fake_ctx = FakeTransport{ .message = message };
    const fake_vtable = netx_transport.VTable{
        .start = FakeTransport.start,
        .poll = FakeTransport.poll,
        .shutdown = FakeTransport.shutdown,
    };
    const transport_instance = netx_transport.Transport.init(&fake_ctx, &fake_vtable);

    const manager = TransportManager{
        .runtime = &runtime,
        .transport = transport_instance,
    };

    var state = SyslogState{
        .allocator = allocator,
        .descriptor = .{ .type = .syslog, .name = "syslog_trunc" },
        .config = config,
        .buffer = undefined,
        .log = null,
        .metrics = &metrics_obj,
        .stream_closed = false,
        .transport = manager,
        .framer = frame.Framer.init(allocator, .auto, config.message_size_limit),
    };
    state.buffer = try EventBuffer.init(allocator, config.max_batch_size, .reject);
    defer state.buffer.deinit();
    defer state.framer.deinit();

    pumpTransport(&state, allocator);

    const managed = state.buffer.pop() orelse return std.testing.expect(false);
    defer managed.finalizer.run();

    try std.testing.expect(managed.event.metadata.payload_truncated);
    const transport_meta = managed.event.metadata.transport orelse return std.testing.expect(false);
    try std.testing.expect(transport_meta == .socket);
    try std.testing.expectEqualStrings("10.0.0.1:5514", transport_meta.socket.peer_address);
    try std.testing.expectEqualStrings("tcp", transport_meta.socket.protocol);

    try std.testing.expectEqual(@as(usize, 1), harness.truncated);
}
