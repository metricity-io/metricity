const std = @import("std");
const collector_mod = @import("collector");
const source_mod = @import("source");
const source_cfg = source_mod.config;
const config_mod = @import("../config/mod.zig");
const sql_mod = @import("../sql/mod.zig");
const sink_mod = @import("sink.zig");
const console_sink = @import("../sink/console.zig");
const event_mod = source_mod.event;
const graph_mod = @import("graph.zig");
const channel_mod = @import("channel.zig");
const metrics_mod = @import("metrics.zig");

const EventChannel = channel_mod.Channel(EventMessage);
const SinkChannel = channel_mod.Channel(SinkMessage);

const backoff_ns: u64 = 5 * std.time.ns_per_ms;
const atomic_order = std.builtin.AtomicOrder;
const max_batch_size: usize = 64;

fn computeBatchCapacity(queue: config_mod.QueueConfig) usize {
    if (queue.capacity == 0) return 1;
    return @min(queue.capacity, max_batch_size);
}

/// Aggregated failure surface for bring-up and steady-state runtime issues within the
/// pipeline engine. Includes collector start/poll errors, SQL compilation/runtime
/// failures, sink backends, acknowledgement failures, allocation pressure and worker
/// lifecycle problems.
pub const Error = collector_mod.CollectorError || sql_mod.runtime.Error || sql_mod.parser.ParseError || sink_mod.Error || event_mod.AckError || std.mem.Allocator.Error || std.Thread.SpawnError || error{ CycleDetected, UnknownSource, ChannelClosed };

/// Fine-grained knobs that allow callers to override collector and sink wiring when
/// instantiating a pipeline.
pub const Options = struct {
    collector: collector_mod.CollectorOptions = .{},
    sink_builder: ?SinkBuilder = null,
    metrics: ?*const source_mod.Metrics = null,
};

/// Callback used to materialise sinks for each configured sink node.
pub const SinkBuilder = *const fn (allocator: std.mem.Allocator, sink: config_mod.SinkNode) sink_mod.Error!sink_mod.Sink;

/// Coordinating runtime that wires sources, transforms and sinks according to the
/// configuration graph. Owns worker threads and synchronises shutdown across the
/// pipeline stages.
pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    collector: collector_mod.Collector,
    collector_configs: []source_cfg.SourceConfig,
    graph: graph_mod.Graph,
    nodes: []NodeRuntime,
    source_lookup: std.StringHashMapUnmanaged(usize) = .{},
    sink_builder: SinkBuilder,
    started: bool = false,
    shutting_down: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    worker_allocator: std.mem.Allocator = std.heap.c_allocator,
    metrics: metrics_mod.PipelineMetrics,

    /// Constructs the pipeline, materialising the collector, topology graph and node
    /// runtimes. No external side-effects (threads, IO) are triggered yet.
    pub fn init(
        allocator: std.mem.Allocator,
        cfg: *const config_mod.PipelineConfig,
        options: Options,
    ) Error!Pipeline {
        var collector_configs = try allocator.alloc(source_cfg.SourceConfig, cfg.sources.len);
        errdefer allocator.free(collector_configs);
        for (cfg.sources, 0..) |src, idx| {
            collector_configs[idx] = src.config;
        }

        var collector = try collector_mod.Collector.init(allocator, collector_configs, options.collector);
        errdefer collector.deinit();

        var graph = try graph_mod.build(allocator, cfg);
        errdefer graph.deinit();

        const nodes = try allocator.alloc(NodeRuntime, graph.nodes.len);
        errdefer allocator.free(nodes);

    var pipeline = Pipeline{
        .allocator = allocator,
        .collector = collector,
        .collector_configs = collector_configs,
        .graph = graph,
        .nodes = nodes,
        .sink_builder = options.sink_builder orelse defaultSinkBuilder,
        .metrics = metrics_mod.PipelineMetrics.init(options.metrics),
    };

        try pipeline.buildSourceLookup();
        try pipeline.initNodes(cfg);
        return pipeline;
    }

    /// Releases all resources associated with the pipeline. Shutdown is attempted even
    /// if the caller forgot to stop the pipeline explicitly.
    pub fn deinit(self: *Pipeline) void {
        self.shutdown() catch {};
        self.source_lookup.deinit(self.allocator);
        for (self.nodes) |*node| {
            node.deinit(self);
        }
        self.allocator.free(self.nodes);
        self.graph.deinit();
        self.collector.deinit();
        self.allocator.free(self.collector_configs);
    }

    /// Starts the collector and launches worker threads for downstream nodes. This
    /// call is idempotent.
    pub fn start(self: *Pipeline) Error!void {
        if (self.started) return;
        try self.collector.start(self.allocator);
        try self.spawnWorkers();
        self.started = true;
    }

    /// Signals all workers to stop, waits for them to complete and shuts down the
    /// collector. Safe to call multiple times.
    pub fn shutdown(self: *Pipeline) Error!void {
        if (!self.started) return;
        const already = self.shutting_down.swap(true, atomic_order.seq_cst);
        if (!already) {
            self.collector.shutdown(self.allocator) catch {};
            for (self.nodes) |*node| node.stop();
            for (self.nodes) |*node| node.join(self);
        }
        self.started = false;
    }

    /// Pulls a single batch from the collector, dispatching contained events through
    /// transforms and sinks. Returns true when a batch yielded work.
    pub fn pollOnce(self: *Pipeline) Error!bool {
        if (!self.started) return false;

        const maybe_batch = self.collector.poll(self.allocator) catch |err| switch (err) {
            collector_mod.CollectorError.Backpressure => {
                std.Thread.sleep(backoff_ns);
                return false;
            },
            else => return err,
        };

        if (maybe_batch) |batch| {
            try self.processBatch(batch);
            return true;
        }
        return false;
    }

    fn buildSourceLookup(self: *Pipeline) !void {
        const required_nodes: u32 = @intCast(self.graph.nodes.len);
        try self.source_lookup.ensureTotalCapacityContext(self.allocator, required_nodes, .{});
        for (self.graph.nodes, 0..) |node, idx| {
            if (node.kind == .source) {
                try self.source_lookup.putNoClobber(self.allocator, node.id, idx);
            }
        }
    }

    fn initNodes(self: *Pipeline, cfg: *const config_mod.PipelineConfig) Error!void {
        for (self.graph.nodes, 0..) |graph_node, idx| {
            const connections = self.graph.connections[idx];
            self.nodes[idx] = NodeRuntime{
                .kind = graph_node.kind,
                .downstream = connections.outgoing,
                .data = undefined,
            };

            switch (graph_node.kind) {
                .source => {
                    self.nodes[idx].data = .{ .source = SourceRuntime{} };
                },
                .transform => {
                    const transform_cfg = &cfg.transforms[graph_node.component.transform];
                    self.nodes[idx].data = .{ .transform = try TransformRuntime.init(self, transform_cfg) };
                },
                .sink => {
                    const sink_cfg = &cfg.sinks[graph_node.component.sink];
                    self.nodes[idx].data = .{ .sink = try SinkRuntime.init(self, sink_cfg) };
                },
            }
        }
    }

    fn spawnWorkers(self: *Pipeline) Error!void {
        for (self.nodes, 0..) |*node, idx| {
            switch (node.data) {
                .transform => |*transform| try transform.spawn(self, idx),
                .sink => |*sink| try sink.spawn(self, idx),
                else => {},
            }
        }
    }

    fn processBatch(self: *Pipeline, batch: collector_mod.PolledBatch) Error!void {
        const source_index = self.source_lookup.get(batch.descriptor.name) orelse return error.UnknownSource;
        const events = batch.batch.events;
        const ack_token = batch.batch.ack;

        const batch_ctx = try BatchContext.create(self.worker_allocator, &self.metrics, ack_token, events.len);
        errdefer batch_ctx.forceRelease();

        if (events.len == 0) {
            if (ack_token.isAvailable()) {
                _ = ack_token.success() catch {};
            }
            batch_ctx.forceRelease();
            return;
        }

        for (events) |*event| {
            var event_ctx = try EventContext.create(self.worker_allocator, batch_ctx);
            var dispatched = false;
            defer if (!dispatched) event_ctx.releaseFailure();
            try self.dispatchEvent(source_index, event, event_ctx);
            dispatched = true;
        }
    }

    fn dispatchEvent(self: *Pipeline, source_index: usize, event: *const event_mod.Event, ctx: *EventContext) Error!void {
        const node = &self.nodes[source_index];
        if (node.downstream.len == 0) {
            ctx.releaseSuccess();
            return;
        }

        for (node.downstream) |target_index| {
            try self.forwardEventToNode(target_index, event, ctx);
        }

        ctx.releaseSuccess();
    }

    fn forwardEventToNode(self: *Pipeline, target_index: usize, event: *const event_mod.Event, ctx: *EventContext) Error!void {
        const node = &self.nodes[target_index];
        switch (node.data) {
            .transform => |*transform| {
                ctx.acquire();
                var message = EventMessage{ .event = event, .context = ctx };
                transform.enqueue(self, &message);
            },
            .sink => |*sink| {
                var row = eventToRow(ctx.batchAllocator(), event) catch |err| {
                    return err;
                };
                ctx.acquire();
                sink.enqueue(self, &row, ctx);
            },
            .source => unreachable,
        }
    }

    fn forwardEventToTransforms(self: *Pipeline, from_index: usize, event: *const event_mod.Event, ctx: *EventContext) void {
        const downstream = self.nodes[from_index].downstream;
        for (downstream) |target_index| {
            const node = &self.nodes[target_index];
            if (node.kind != .transform) continue;
            ctx.acquire();
            var message = EventMessage{ .event = event, .context = ctx };
            node.data.transform.enqueue(self, &message);
        }
    }

    fn sendRowFromTransform(self: *Pipeline, from_index: usize, row: sql_mod.runtime.Row, ctx: *EventContext) Error!void {
        const downstream = self.nodes[from_index].downstream;
        var sink_total: usize = 0;
        for (downstream) |idx| {
            if (self.nodes[idx].kind == .sink) sink_total += 1;
        }
        if (sink_total == 0) {
            row.deinit();
            return;
        }

        var delivered: usize = 0;
        var original = row;
        for (downstream) |target_index| {
            const node = &self.nodes[target_index];
            if (node.kind != .sink) continue;
            delivered += 1;
            if (delivered == sink_total) {
                ctx.acquire();
                node.data.sink.enqueue(self, &original, ctx);
            } else {
                var clone = cloneRow(ctx.batchAllocator(), original) catch |err| {
                    original.deinit();
                    return err;
                };
                ctx.acquire();
                node.data.sink.enqueue(self, &clone, ctx);
            }
        }
    }
};

const NodeRuntime = struct {
    kind: graph_mod.NodeKind,
    downstream: []usize,
    data: NodeData,

    fn deinit(self: *NodeRuntime, pipeline: *Pipeline) void {
        switch (self.data) {
            .transform => |*transform| transform.deinit(pipeline),
            .sink => |*sink| sink.deinit(pipeline),
            .source => {},
        }
    }

    fn stop(self: *NodeRuntime) void {
        switch (self.data) {
            .transform => |*transform| transform.stop(),
            .sink => |*sink| sink.stop(),
            .source => {},
        }
    }

    fn join(self: *NodeRuntime, pipeline: *Pipeline) void {
        switch (self.data) {
            .transform => |*transform| transform.join(pipeline),
            .sink => |*sink| sink.join(pipeline),
            .source => {},
        }
    }
};

const NodeData = union(enum) {
    source: SourceRuntime,
    transform: TransformRuntime,
    sink: SinkRuntime,
};

const SourceRuntime = struct {};

const EventMessage = struct {
    event: *const event_mod.Event,
    context: *EventContext,
};

const SinkMessage = struct {
    row: sql_mod.runtime.Row,
    context: *EventContext,
};

const TransformRuntime = struct {
    channel: EventChannel,
    queue: config_mod.QueueConfig,
    workers: []TransformWorker,
    batch_capacity: usize,
    arena: *std.heap.ArenaAllocator,
    program: sql_mod.runtime.Program,
    config: *const config_mod.TransformNode,
    channel_metrics: metrics_mod.ChannelMetrics,

    fn init(pipeline: *Pipeline, config: *const config_mod.TransformNode) Error!TransformRuntime {
        const exec = config.executionSettings();
        var channel_metrics = try pipeline.metrics.createChannelMetrics(
            pipeline.allocator,
            .{ .name = config.id(), .kind = .transform },
        );
        errdefer channel_metrics.deinit();

        var channel = try EventChannel.init(pipeline.worker_allocator, exec.queue, channel_metrics.observer());
        errdefer channel.deinit();

        var arena = try pipeline.allocator.create(std.heap.ArenaAllocator);
        errdefer pipeline.allocator.destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(pipeline.allocator);
        errdefer arena.deinit();

        const sql_cfg = switch (config.*) {
            .sql => |value| value,
        };

        const stmt = try sql_mod.parser.parseSelect(arena.allocator(), sql_cfg.query);
        var program = try sql_mod.runtime.compile(pipeline.worker_allocator, stmt);
        errdefer program.deinit();

        const workers = try pipeline.allocator.alloc(TransformWorker, exec.parallelism);
        errdefer pipeline.allocator.free(workers);
        for (workers) |*worker| worker.* = .{};

        return TransformRuntime{
            .channel = channel,
            .queue = exec.queue,
            .workers = workers,
            .batch_capacity = computeBatchCapacity(exec.queue),
            .arena = arena,
            .program = program,
            .config = config,
            .channel_metrics = channel_metrics,
        };
    }

    fn spawn(self: *TransformRuntime, pipeline: *Pipeline, node_index: usize) Error!void {
        for (self.workers) |*worker| {
            const context = try pipeline.allocator.create(TransformWorkerContext);
            errdefer pipeline.allocator.destroy(context);
            context.* = try TransformWorkerContext.init(pipeline, node_index, self);
            errdefer context.deinit();
            worker.context = context;
            worker.thread = try std.Thread.spawn(.{}, transformWorkerMain, .{context});
        }
    }

    fn enqueue(self: *TransformRuntime, pipeline: *Pipeline, message: *EventMessage) void {
        var local = message.*;
        retryPushEvent(&self.channel, pipeline, &local);
    }

    fn stop(self: *TransformRuntime) void {
        self.channel.close();
    }

    fn join(self: *TransformRuntime, pipeline: *Pipeline) void {
        for (self.workers) |*worker| {
            if (worker.thread) |thread| thread.join();
            worker.thread = null;
            if (worker.context) |context| {
                context.deinit();
                pipeline.allocator.destroy(context);
                worker.context = null;
            }
        }
    }

    fn deinit(self: *TransformRuntime, pipeline: *Pipeline) void {
        self.program.deinit();
        self.arena.deinit();
        pipeline.allocator.destroy(self.arena);
        self.channel.deinit();
        pipeline.allocator.free(self.workers);
        self.channel_metrics.deinit();
    }
};

const TransformWorker = struct {
    thread: ?std.Thread = null,
    context: ?*TransformWorkerContext = null,
};

const TransformWorkerContext = struct {
    pipeline: *Pipeline,
    node_index: usize,
    runtime: *TransformRuntime,
    batch_buffer: []EventMessage,

    fn init(pipeline: *Pipeline, node_index: usize, runtime: *TransformRuntime) !TransformWorkerContext {
        const capacity = runtime.batch_capacity;
        const buffer = try pipeline.allocator.alloc(EventMessage, capacity);
        return TransformWorkerContext{
            .pipeline = pipeline,
            .node_index = node_index,
            .runtime = runtime,
            .batch_buffer = buffer,
        };
    }

    fn deinit(self: *TransformWorkerContext) void {
        self.pipeline.allocator.free(self.batch_buffer);
    }
};

fn transformWorkerMain(context: *TransformWorkerContext) void {
    while (true) {
        const count = context.runtime.channel.popBatch(context.batch_buffer);
        if (count == 0) break;
        for (context.batch_buffer[0..count]) |message| {
            handleTransformMessage(context, message);
        }
    }
}

fn handleTransformMessage(context: *TransformWorkerContext, message: EventMessage) void {
    const pipeline = context.pipeline;
    const runtime = context.runtime;
    const node_index = context.node_index;

    const produced = runtime.program.execute(message.context.batchAllocator(), message.event) catch {
        message.context.releaseFailure();
        return;
    };

    if (produced) |row| {
        // Row fan-out only targets sink nodes. The engine does not currently feed
        // produced rows into downstream transforms, which means multi-stage
        // transform chains only share the original event payload rather than
        // composing intermediate rows.
        pipeline.sendRowFromTransform(node_index, row, message.context) catch {
            message.context.releaseFailure();
            return;
        };
    }

    // Downstream transforms are invoked with the original event rather than the
    // materialised row produced above. This keeps transform execution side-effect
    // free and clarifies that chaining transforms today does not compose results.
    pipeline.forwardEventToTransforms(node_index, message.event, message.context);

    message.context.releaseSuccess();
}

const SinkRuntime = struct {
    channel: SinkChannel,
    queue: config_mod.QueueConfig,
    workers: []SinkWorker,
    batch_capacity: usize,
    config: *const config_mod.SinkNode,
    channel_metrics: metrics_mod.ChannelMetrics,

    fn init(pipeline: *Pipeline, config: *const config_mod.SinkNode) Error!SinkRuntime {
        const exec = config.executionSettings();
        var channel_metrics = try pipeline.metrics.createChannelMetrics(
            pipeline.allocator,
            .{ .name = config.id(), .kind = .sink },
        );
        errdefer channel_metrics.deinit();

        var channel = try SinkChannel.init(pipeline.worker_allocator, exec.queue, channel_metrics.observer());
        errdefer channel.deinit();

        const workers = try pipeline.allocator.alloc(SinkWorker, exec.parallelism);
        errdefer pipeline.allocator.free(workers);
        for (workers) |*worker| worker.* = .{};

        return SinkRuntime{
            .channel = channel,
            .queue = exec.queue,
            .workers = workers,
            .batch_capacity = computeBatchCapacity(exec.queue),
            .config = config,
            .channel_metrics = channel_metrics,
        };
    }

    fn spawn(self: *SinkRuntime, pipeline: *Pipeline, node_index: usize) Error!void {
        for (self.workers) |*worker| {
            const context = try pipeline.allocator.create(SinkWorkerContext);
            errdefer pipeline.allocator.destroy(context);
            context.* = try SinkWorkerContext.init(pipeline, self.config, self.batch_capacity);
            errdefer context.deinit();
            context.node_index = node_index;
            worker.context = context;
            worker.thread = try std.Thread.spawn(.{}, sinkWorkerMain, .{context});
        }
    }

    fn enqueue(self: *SinkRuntime, pipeline: *Pipeline, row: *sql_mod.runtime.Row, ctx: *EventContext) void {
        var message = SinkMessage{ .row = row.*, .context = ctx };
        row.* = undefined;
        retryPushSink(&self.channel, pipeline, &message);
    }

    fn stop(self: *SinkRuntime) void {
        self.channel.close();
    }

    fn join(self: *SinkRuntime, pipeline: *Pipeline) void {
        for (self.workers) |*worker| {
            if (worker.thread) |thread| thread.join();
            worker.thread = null;
            if (worker.context) |context| {
                context.deinit();
                pipeline.allocator.destroy(context);
                worker.context = null;
            }
        }
    }

    fn deinit(self: *SinkRuntime, pipeline: *Pipeline) void {
        self.channel.deinit();
        pipeline.allocator.free(self.workers);
        self.channel_metrics.deinit();
    }
};

const SinkWorker = struct {
    thread: ?std.Thread = null,
    context: ?*SinkWorkerContext = null,
};

const SinkWorkerContext = struct {
    pipeline: *Pipeline,
    node_index: usize,
    sink: sink_mod.Sink,
    batch_buffer: []SinkMessage,
    row_view: []sql_mod.runtime.Row,

    fn init(pipeline: *Pipeline, config: *const config_mod.SinkNode, batch_capacity: usize) sink_mod.Error!SinkWorkerContext {
        const sink_instance = try pipeline.sink_builder(pipeline.allocator, config.*);
        errdefer sink_instance.deinit(pipeline.allocator);

        const buffer = try pipeline.allocator.alloc(SinkMessage, batch_capacity);
        errdefer pipeline.allocator.free(buffer);

        const row_view = try pipeline.allocator.alloc(sql_mod.runtime.Row, batch_capacity);

        return SinkWorkerContext{
            .pipeline = pipeline,
            .node_index = 0,
            .sink = sink_instance,
            .batch_buffer = buffer,
            .row_view = row_view,
        };
    }

    fn deinit(self: *SinkWorkerContext) void {
        self.pipeline.allocator.free(self.row_view);
        self.pipeline.allocator.free(self.batch_buffer);
        self.sink.flush() catch {};
        self.sink.deinit(self.pipeline.allocator);
    }
};

fn sinkWorkerMain(context: *SinkWorkerContext) void {
    const runtime = &context.pipeline.nodes[context.node_index].data.sink;

    while (true) {
        const count = runtime.channel.popBatch(context.batch_buffer);
        if (count == 0) break;
        const messages = context.batch_buffer[0..count];
        var rows = context.row_view[0..count];
        for (messages, 0..) |message, idx| {
            rows[idx] = message.row;
        }

        context.sink.emitBatch(rows) catch {
            for (messages) |message| {
                var failed_row = message.row;
                failed_row.deinit();
                message.context.releaseFailure();
            }
            continue;
        };

        for (messages) |message| {
            var success_row = message.row;
            success_row.deinit();
            message.context.releaseSuccess();
        }
    }
}

fn retryPushEvent(channel: *EventChannel, pipeline: *Pipeline, message: *EventMessage) void {
    var local = message.*;
    if (pipeline.shutting_down.load(atomic_order.seq_cst)) {
        local.context.releaseFailure();
        return;
    }

    const outcome = channel.pushBlocking(local) catch |err| switch (err) {
        channel_mod.PushError.Closed => {
            local.context.releaseFailure();
            return;
        },
        channel_mod.PushError.WouldBlock => unreachable,
    };

    switch (outcome) {
        .stored => return,
        .dropped_newest => {
            local.context.releaseFailure();
        },
        .dropped_oldest => |evicted| {
            evicted.context.releaseFailure();
        },
    }
}

fn retryPushSink(channel: *SinkChannel, pipeline: *Pipeline, message: *SinkMessage) void {
    var local = message.*;
    if (pipeline.shutting_down.load(atomic_order.seq_cst)) {
        local.row.deinit();
        local.context.releaseFailure();
        return;
    }

    const outcome = channel.pushBlocking(local) catch |err| switch (err) {
        channel_mod.PushError.Closed => {
            local.row.deinit();
            local.context.releaseFailure();
            return;
        },
        channel_mod.PushError.WouldBlock => unreachable,
    };

    switch (outcome) {
        .stored => return,
        .dropped_newest => {
            local.row.deinit();
            local.context.releaseFailure();
        },
        .dropped_oldest => |evicted| {
            evicted.row.deinit();
            evicted.context.releaseFailure();
        },
    }
}

const BatchContext = struct {
    allocator: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    thread_safe: *std.heap.ThreadSafeAllocator,
    ack: event_mod.AckToken,
    remaining: std.atomic.Value(usize),
    failed: std.atomic.Value(bool),
    metrics: *const metrics_mod.PipelineMetrics,
    start_time: i128,

    fn create(
        allocator: std.mem.Allocator,
        metrics: *const metrics_mod.PipelineMetrics,
        ack: event_mod.AckToken,
        total_events: usize,
    ) !*BatchContext {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        errdefer allocator.destroy(arena);

        arena.* = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();

        const thread_safe = try allocator.create(std.heap.ThreadSafeAllocator);
        errdefer allocator.destroy(thread_safe);
        thread_safe.* = .{ .child_allocator = arena.allocator() };

        const ctx = try allocator.create(BatchContext);
        ctx.* = .{
            .allocator = allocator,
            .arena = arena,
            .thread_safe = thread_safe,
            .ack = ack,
            .remaining = std.atomic.Value(usize).init(total_events),
            .failed = std.atomic.Value(bool).init(false),
            .metrics = metrics,
            .start_time = std.time.nanoTimestamp(),
        };
        return ctx;
    }

    fn allocatorForBatch(self: *BatchContext) std.mem.Allocator {
        return self.thread_safe.allocator();
    }

    fn complete(self: *BatchContext, success: bool) void {
        if (!success) {
            self.failed.store(true, atomic_order.seq_cst);
        }
        const previous = self.remaining.fetchSub(1, atomic_order.seq_cst);
        if (previous == 1) {
            if (self.ack.isAvailable()) {
                if (self.failed.load(atomic_order.seq_cst)) {
                    _ = self.ack.failRetryable() catch {};
                } else {
                    _ = self.ack.success() catch {};
                }
            }
            const final_status: event_mod.AckStatus = if (self.failed.load(atomic_order.seq_cst) or !success)
                event_mod.AckStatus.retryable_failure
            else
                event_mod.AckStatus.success;
            const now = std.time.nanoTimestamp();
            const raw_latency = now - self.start_time;
            const latency_u64: u64 = if (raw_latency >= 0)
                @intCast(raw_latency)
            else
                @intCast(-raw_latency);
            self.metrics.recordAck(final_status);
            self.metrics.recordAckLatency(latency_u64);
            self.destroy();
        }
    }

    fn reset(self: *BatchContext) void {
        self.arena.deinit();
        self.arena.* = std.heap.ArenaAllocator.init(self.allocator);
        self.thread_safe.* = .{ .child_allocator = self.arena.allocator() };
    }

    fn destroy(self: *BatchContext) void {
        self.arena.deinit();
        self.allocator.destroy(self.thread_safe);
        self.allocator.destroy(self.arena);
        self.allocator.destroy(self);
    }

    fn forceRelease(self: *BatchContext) void {
        self.destroy();
    }
};

const EventContext = struct {
    batch: *BatchContext,
    ref_count: std.atomic.Value(usize),
    failed: std.atomic.Value(bool),

    fn create(allocator: std.mem.Allocator, batch: *BatchContext) !*EventContext {
        const ctx = try allocator.create(EventContext);
        ctx.* = .{
            .batch = batch,
            .ref_count = std.atomic.Value(usize).init(1),
            .failed = std.atomic.Value(bool).init(false),
        };
        return ctx;
    }

    fn acquire(self: *EventContext) void {
        _ = self.ref_count.fetchAdd(1, atomic_order.seq_cst);
    }

    fn batchAllocator(self: *EventContext) std.mem.Allocator {
        return self.batch.allocatorForBatch();
    }

    fn releaseSuccess(self: *EventContext) void {
        self.release(true);
    }

    fn releaseFailure(self: *EventContext) void {
        self.failed.store(true, atomic_order.seq_cst);
        self.release(false);
    }

    fn release(self: *EventContext, success: bool) void {
        const previous = self.ref_count.fetchSub(1, atomic_order.seq_cst);
        if (previous == 1) {
            const batch = self.batch;
            const allocator = batch.allocator;
            const final_success = success and !self.failed.load(atomic_order.seq_cst);
            batch.complete(final_success);
            allocator.destroy(self);
        }
    }
};

fn cloneRow(allocator: std.mem.Allocator, source: sql_mod.runtime.Row) !sql_mod.runtime.Row {
    var entries = try allocator.alloc(sql_mod.runtime.ValueEntry, source.values.len);
    for (source.values, 0..) |value, idx| {
        entries[idx] = value;
    }
    return sql_mod.runtime.Row{ .allocator = allocator, .values = entries };
}

fn eventToRow(allocator: std.mem.Allocator, event: *const event_mod.Event) !sql_mod.runtime.Row {
    switch (event.payload) {
        .log => |log_event| {
            const total = log_event.fields.len + 1;
            var entries = try allocator.alloc(sql_mod.runtime.ValueEntry, total);
            errdefer allocator.free(entries);

            entries[0] = .{ .name = "message", .value = .{ .string = log_event.message } };
            for (log_event.fields, 0..) |field, idx| {
                entries[idx + 1] = .{ .name = field.name, .value = field.value };
            }

            return sql_mod.runtime.Row{ .allocator = allocator, .values = entries };
        },
    }
}

fn defaultSinkBuilder(allocator: std.mem.Allocator, node: config_mod.SinkNode) sink_mod.Error!sink_mod.Sink {
    return switch (node) {
        .console => |cfg| console_sink.build(allocator, cfg),
    };
}

const testing = std.testing;

const TestSink = struct {
    const Context = struct {
        mutex: std.Thread.Mutex = .{},
        seen: bool = false,
        value: ?i64 = null,
        message: ?[]const u8 = null,
    };

    const Snapshot = struct {
        seen: bool,
        value: ?i64,
        message: ?[]const u8,
    };

    var shared_context: ?*Context = null;

    fn builder(allocator: std.mem.Allocator, _: config_mod.SinkNode) sink_mod.Error!sink_mod.Sink {
        const ctx = try allocator.create(Context);
        ctx.* = .{};
        shared_context = ctx;
        return sink_mod.Sink{ .context = ctx, .vtable = &vtable };
    }

    fn emit(context: *anyopaque, row: *const sql_mod.runtime.Row) sink_mod.Error!void {
        const ctx: *Context = @ptrCast(@alignCast(context));
        ctx.mutex.lock();
        defer ctx.mutex.unlock();
        ctx.seen = true;
        if (row.values.len > 0 and row.values[0].value == .integer) {
            ctx.value = row.values[0].value.integer;
        }
        if (row.values.len > 1 and row.values[1].value == .string) {
            ctx.message = row.values[1].value.string;
        }
    }

    fn flush(_: *anyopaque) sink_mod.Error!void {
        return;
    }

    fn deinit(context: *anyopaque, allocator: std.mem.Allocator) void {
        const ctx: *Context = @ptrCast(@alignCast(context));
        allocator.destroy(ctx);
    }

    fn snapshot() Snapshot {
        const ctx = shared_context orelse return Snapshot{ .seen = false, .value = null, .message = null };
        ctx.mutex.lock();
        const snap = Snapshot{ .seen = ctx.seen, .value = ctx.value, .message = ctx.message };
        ctx.mutex.unlock();
        return snap;
    }

    fn reset() void {
        shared_context = null;
    }

    const vtable = sink_mod.VTable{
        .emit = emit,
        .flush = flush,
        .deinit = deinit,
    };
};

const CountingSink = struct {
    const Context = struct {};

    var counter = std.atomic.Value(usize).init(0);

    fn builder(allocator: std.mem.Allocator, _: config_mod.SinkNode) sink_mod.Error!sink_mod.Sink {
        const ctx = try allocator.create(Context);
        ctx.* = .{};
        return sink_mod.Sink{ .context = ctx, .vtable = &vtable };
    }

    fn emit(_: *anyopaque, _: *const sql_mod.runtime.Row) sink_mod.Error!void {
        _ = counter.fetchAdd(1, atomic_order.seq_cst);
    }

    fn flush(_: *anyopaque) sink_mod.Error!void {
        return;
    }

    fn deinit(context: *anyopaque, allocator: std.mem.Allocator) void {
        const ctx: *Context = @ptrCast(@alignCast(context));
        allocator.destroy(ctx);
    }

    fn reset() void {
        counter.store(0, atomic_order.seq_cst);
    }

    const vtable = sink_mod.VTable{
        .emit = emit,
        .flush = flush,
        .deinit = deinit,
    };
};

const TestSource = struct {
    const State = struct {
        emitted: bool = false,
        event_storage: [1]event_mod.Event,
        fields: []event_mod.Field,
    };

    pub fn factory() source_mod.SourceFactory {
        return .{ .type = .syslog, .create = create };
    }

    fn create(ctx: source_mod.InitContext, config: *const source_cfg.SourceConfig) source_mod.SourceError!source_mod.Source {
        const state = ctx.allocator.create(State) catch return source_mod.SourceError.StartupFailed;
        errdefer ctx.allocator.destroy(state);

        state.* = .{
            .emitted = false,
            .event_storage = undefined,
            .fields = &[_]event_mod.Field{},
        };

        state.fields = ctx.allocator.alloc(event_mod.Field, 2) catch return source_mod.SourceError.StartupFailed;
        errdefer ctx.allocator.free(state.fields);
        state.fields[0] = .{ .name = "value", .value = .{ .integer = 41 } };
        state.fields[1] = .{ .name = "level", .value = .{ .string = "info" } };

        state.event_storage[0] = event_mod.Event{
            .metadata = .{ .source_id = config.id },
            .payload = .{ .log = .{ .message = "hello", .fields = state.fields } },
        };

        return source_mod.Source{
            .descriptor = .{ .type = .syslog, .name = config.id },
            .capabilities = .{ .streaming = false, .batching = true },
            .lifecycle = .{
                .start_stream = startStream,
                .poll_batch = pollBatch,
                .shutdown = shutdown,
            },
            .context = state,
        };
    }

    fn startStream(_: *anyopaque, _: std.mem.Allocator) source_mod.SourceError!?event_mod.EventStream {
        return source_mod.SourceError.OperationNotSupported;
    }

    fn pollBatch(context: *anyopaque, _: std.mem.Allocator) source_mod.SourceError!?event_mod.EventBatch {
        const state: *State = @ptrCast(@alignCast(context));
        if (state.emitted) return null;
        state.emitted = true;
        return event_mod.EventBatch{ .events = state.event_storage[0..1], .ack = event_mod.AckToken.none() };
    }

    fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
        const state: *State = @ptrCast(@alignCast(context));
        allocator.free(state.fields);
        allocator.destroy(state);
    }
};

test "pipeline applies sql transform and emits to sink" {
    TestSink.reset();
    const allocator = testing.allocator;

    const source_config = source_cfg.SourceConfig{
        .id = "test_source",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:0" } },
    };

    const pipeline_cfg = config_mod.PipelineConfig{
        .sources = &[_]config_mod.SourceNode{
            .{ .id = "test_source", .config = source_config, .outputs = &[_][]const u8{"sql"} },
        },
        .transforms = &[_]config_mod.TransformNode{
            .{ .sql = .{
                .id = "sql",
                .inputs = &[_][]const u8{"test_source"},
                .outputs = &[_][]const u8{"sink"},
                .query = "SELECT value + 1 AS next_value, message FROM logs WHERE level = 'info'",
            } },
        },
        .sinks = &[_]config_mod.SinkNode{
            .{ .console = .{ .id = "sink", .inputs = &[_][]const u8{"sql"} } },
        },
    };

    const registry = source_mod.Registry{ .factories = &[_]source_mod.SourceFactory{TestSource.factory()} };

    var pipeline = try Pipeline.init(allocator, &pipeline_cfg, .{ .collector = .{ .registry = registry }, .sink_builder = TestSink.builder });
    defer pipeline.deinit();

    try pipeline.start();

    const processed = try pipeline.pollOnce();
    try testing.expect(processed);

    var attempts: usize = 0;
    while (attempts < 100) : (attempts += 1) {
        const snapshot = TestSink.snapshot();
        if (snapshot.seen) {
            try testing.expectEqual(@as(?i64, 42), snapshot.value);
            try testing.expectEqualStrings("hello", snapshot.message orelse "");
            break;
        }
        std.Thread.sleep(1 * std.time.ns_per_ms);
    } else {
        try testing.expect(false); // never observed
    }

    try pipeline.shutdown();
    TestSink.reset();
}

test "pipeline fan-out to multiple sinks" {
    CountingSink.reset();
    const allocator = testing.allocator;

    const source_config = source_cfg.SourceConfig{
        .id = "fan_source",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:0" } },
    };

    const pipeline_cfg = config_mod.PipelineConfig{
        .sources = &[_]config_mod.SourceNode{
            .{ .id = "fan_source", .config = source_config, .outputs = &[_][]const u8{ "sink_a", "sink_b" } },
        },
        .transforms = &[_]config_mod.TransformNode{},
        .sinks = &[_]config_mod.SinkNode{
            .{ .console = .{ .id = "sink_a", .inputs = &[_][]const u8{"fan_source"} } },
            .{ .console = .{ .id = "sink_b", .inputs = &[_][]const u8{"fan_source"} } },
        },
    };

    const registry = source_mod.Registry{ .factories = &[_]source_mod.SourceFactory{TestSource.factory()} };

    var pipeline = try Pipeline.init(allocator, &pipeline_cfg, .{ .collector = .{ .registry = registry }, .sink_builder = CountingSink.builder });
    defer pipeline.deinit();

    try pipeline.start();
    const processed = try pipeline.pollOnce();
    try testing.expect(processed);

    var attempts: usize = 0;
    while (attempts < 100 and CountingSink.counter.load(atomic_order.seq_cst) != 2) : (attempts += 1) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }
    try testing.expectEqual(@as(usize, 2), CountingSink.counter.load(atomic_order.seq_cst));

    try pipeline.shutdown();
    CountingSink.reset();
}

test "forwardEventToNode surfaces allocation failure without double release" {
    const allocator = testing.allocator;

    var pipeline = Pipeline{
        .allocator = allocator,
        .collector = undefined,
        .collector_configs = &[_]source_cfg.SourceConfig{},
        .graph = undefined,
        .nodes = undefined,
        .source_lookup = .{},
        .sink_builder = defaultSinkBuilder,
        .started = false,
        .shutting_down = std.atomic.Value(bool).init(false),
        .worker_allocator = allocator,
        .metrics = metrics_mod.PipelineMetrics.init(null),
    };

    var nodes = [_]NodeRuntime{
        .{
            .kind = .sink,
            .downstream = &[_]usize{},
            .data = .{ .sink = undefined },
        },
    };
    pipeline.nodes = &nodes;

    const Recorder = struct {
        status: ?event_mod.AckStatus = null,
        calls: usize = 0,

        fn complete(context: *anyopaque, status: event_mod.AckStatus) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.status = status;
            self.calls += 1;
        }
    };

    var recorder = Recorder{};
    var handle = event_mod.AckHandle.init(&recorder, Recorder.complete);
    const token = event_mod.AckToken.init(&handle);

    var batch_ctx = try BatchContext.create(pipeline.worker_allocator, &pipeline.metrics, token, 1);
    errdefer batch_ctx.forceRelease();

    var fail_storage: [1]u8 = undefined;
    var failing_allocator = std.heap.FixedBufferAllocator.init(fail_storage[0..0]);
    batch_ctx.arena.deinit();
    batch_ctx.arena.* = std.heap.ArenaAllocator.init(failing_allocator.allocator());
    batch_ctx.thread_safe.* = .{ .child_allocator = batch_ctx.arena.allocator() };

    var event_ctx = try EventContext.create(pipeline.worker_allocator, batch_ctx);

    const event = event_mod.Event{
        .metadata = .{},
        .payload = .{ .log = .{ .message = "msg", .fields = &[_]event_mod.Field{} } },
    };

    try testing.expectError(error.OutOfMemory, pipeline.forwardEventToNode(0, &event, event_ctx));
    event_ctx.releaseFailure();

    try testing.expectEqual(@as(usize, 1), recorder.calls);
    const status = recorder.status orelse unreachable;
    try testing.expectEqual(event_mod.AckStatus.retryable_failure, status);
}

test "retryPushEvent treats drop_oldest as stored" {
    const allocator = testing.allocator;

    var pipeline = Pipeline{
        .allocator = allocator,
        .collector = undefined,
        .collector_configs = &[_]source_cfg.SourceConfig{},
        .graph = undefined,
        .nodes = undefined,
        .source_lookup = .{},
        .sink_builder = defaultSinkBuilder,
        .started = false,
        .shutting_down = std.atomic.Value(bool).init(false),
        .worker_allocator = allocator,
        .metrics = metrics_mod.PipelineMetrics.init(null),
    };

    var channel = try EventChannel.init(allocator, .{ .capacity = 1, .strategy = .drop_oldest }, null);
    defer channel.deinit();

    const Recorder = struct {
        status: ?event_mod.AckStatus = null,
        calls: usize = 0,

        fn complete(context: *anyopaque, status: event_mod.AckStatus) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.status = status;
            self.calls += 1;
        }
    };

    var recorder = Recorder{};
    var handle = event_mod.AckHandle.init(&recorder, Recorder.complete);
    const token = event_mod.AckToken.init(&handle);

    var batch_ctx = try BatchContext.create(allocator, &pipeline.metrics, token, 2);
    errdefer batch_ctx.forceRelease();

    var ctx_old = try EventContext.create(allocator, batch_ctx);
    errdefer ctx_old.releaseFailure();
    var ctx_new = try EventContext.create(allocator, batch_ctx);
    errdefer ctx_new.releaseFailure();

    const event = event_mod.Event{
        .metadata = .{},
        .payload = .{ .log = .{ .message = "drop", .fields = &[_]event_mod.Field{} } },
    };

    ctx_old.acquire();
    var message_old = EventMessage{ .event = &event, .context = ctx_old };
    retryPushEvent(&channel, &pipeline, &message_old);

    ctx_new.acquire();
    var message_new = EventMessage{ .event = &event, .context = ctx_new };
    retryPushEvent(&channel, &pipeline, &message_new);

    const popped = channel.pop() orelse unreachable;
    try testing.expect(popped.context == ctx_new);

    popped.context.releaseSuccess();
    ctx_old.releaseFailure();
    ctx_new.releaseSuccess();

    try testing.expectEqual(@as(usize, 1), recorder.calls);
    const status = recorder.status orelse unreachable;
    try testing.expectEqual(event_mod.AckStatus.retryable_failure, status);
}

test "batch context records ack metrics" {
    const allocator = testing.allocator;

    const Recorder = struct {
        success: u64 = 0,
        retryable: u64 = 0,
        permanent: u64 = 0,
        latency_total: u64 = 0,
        latency_count: u64 = 0,

        fn incr(context: *anyopaque, name: []const u8, value: u64) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (std.mem.eql(u8, name, "pipeline_ack_success_total")) {
                self.success += value;
                return;
            }
            if (std.mem.eql(u8, name, "pipeline_ack_retryable_total")) {
                self.retryable += value;
                return;
            }
            if (std.mem.eql(u8, name, "pipeline_ack_permanent_total")) {
                self.permanent += value;
                return;
            }
            if (std.mem.eql(u8, name, "pipeline_ack_latency_ns_total")) {
                self.latency_total += value;
                return;
            }
            if (std.mem.eql(u8, name, "pipeline_ack_latency_events_total")) {
                self.latency_count += value;
                return;
            }
        }
    };

    var recorder = Recorder{};
    const metrics_sink = source_mod.Metrics{
        .context = &recorder,
        .incr_counter_fn = Recorder.incr,
        .record_gauge_fn = null,
    };

    var pipeline_metrics = metrics_mod.PipelineMetrics.init(&metrics_sink);

    var success_ctx = try BatchContext.create(allocator, &pipeline_metrics, event_mod.AckToken.none(), 1);
    std.Thread.sleep(1 * std.time.ns_per_ms);
    success_ctx.complete(true);

    var failure_ctx = try BatchContext.create(allocator, &pipeline_metrics, event_mod.AckToken.none(), 1);
    std.Thread.sleep(1 * std.time.ns_per_ms);
    failure_ctx.complete(false);

    try testing.expectEqual(@as(u64, 1), recorder.success);
    try testing.expectEqual(@as(u64, 1), recorder.retryable);
    try testing.expectEqual(@as(u64, 0), recorder.permanent);
    try testing.expectEqual(@as(u64, 2), recorder.latency_count);
    try testing.expect(recorder.latency_total >= recorder.latency_count);
}
