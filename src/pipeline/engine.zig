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
const wyhash = std.hash.Wyhash;

const EventChannel = channel_mod.Channel(EventMessage);
const SinkChannel = channel_mod.Channel(SinkMessage);

const backoff_ns: u64 = 5 * std.time.ns_per_ms;
const atomic_order = std.builtin.AtomicOrder;
const max_batch_size: usize = 64;

/// Provides monotonically non-decreasing nanosecond timestamps for SQL eviction.
/// Prefers `std.time.Instant` (monotonic clock) and falls back to clamped wall time.
const MonotonicClock = struct {
    var once = std.once(init);
    var base: std.time.Instant = undefined;
    var supported: bool = false;
    var last_ns = std.atomic.Value(u64).init(0);

    fn init() void {
        const Self = @This();
        const instant = std.time.Instant.now() catch {
            Self.supported = false;
            return;
        };
        Self.base = instant;
        Self.supported = true;
    }

    fn now() u64 {
        const Self = @This();
        Self.once.call();
        if (Self.supported) {
            const current = std.time.Instant.now() catch return Self.fallback();
            const elapsed = current.since(Self.base);
            return Self.enforce(elapsed);
        }
        return Self.fallback();
    }

    fn enforce(candidate: u64) u64 {
        const Self = @This();
        const previous = Self.last_ns.fetchMax(candidate, .acq_rel);
        return if (candidate > previous) candidate else previous;
    }

    fn fallback() u64 {
        const Self = @This();
        const raw = std.time.nanoTimestamp();
        const positive = if (raw < 0) 0 else @as(u128, @intCast(raw));
        const clamped = std.math.cast(u64, positive) orelse std.math.maxInt(u64);
        return Self.enforce(clamped);
    }
};

fn computeBatchCapacity(queue: config_mod.QueueConfig) usize {
    if (queue.capacity == 0) return 1;
    return @min(queue.capacity, max_batch_size);
}

fn secondsToNanoseconds(seconds: u64) u64 {
    if (seconds == 0) return 0;
    const product = @as(u128, seconds) * std.time.ns_per_s;
    if (product > std.math.maxInt(u64)) {
        return std.math.maxInt(u64);
    }
    return @intCast(product);
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
            for (self.nodes) |*node| node.stop();
            for (self.nodes) |*node| node.join(self);
            self.collector.shutdown(self.allocator) catch {};
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
                    self.nodes[idx].data.transform.attachObserver();
                },
                .sink => {
                    const sink_cfg = &cfg.sinks[graph_node.component.sink];
                    self.nodes[idx].data = .{ .sink = try SinkRuntime.init(self, sink_cfg) };
                    self.nodes[idx].data.sink.attachObserver();
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

        const batch_ctx = try BatchContext.create(self.worker_allocator, &self.metrics, ack_token, events.len, batch.descriptor.name);
        errdefer batch_ctx.forceRelease();

        if (events.len != 0) {
            const count: u64 = @intCast(events.len);
            self.metrics.recordSourceIngest(batch.descriptor.name, count);
        }

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

    fn hasDownstreamTransform(self: *Pipeline, from_index: usize) bool {
        const downstream = self.nodes[from_index].downstream;
        for (downstream) |target_index| {
            if (self.nodes[target_index].kind == .transform) return true;
        }
        return false;
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

const TransformShardRuntime = struct {
    name: []const u8,
    name_owned: bool,
    channel: EventChannel,
    channel_metrics: metrics_mod.ChannelMetrics,
    batch_capacity: usize,
    arena: *std.heap.ArenaAllocator,
    program: sql_mod.runtime.Program,
    program_mutex: std.Thread.Mutex = .{},
    workers: []TransformWorker,

    fn deinit(self: *TransformShardRuntime, pipeline: *Pipeline) void {
        self.channel.drainWith(struct {
            fn release(message: EventMessage) void {
                message.context.releaseFailure();
            }
        }.release);
        self.channel.deinit();
        pipeline.allocator.free(self.workers);
        self.channel_metrics.deinit();
        self.program.deinit();
        self.arena.deinit();
        pipeline.allocator.destroy(self.arena);
        if (self.name_owned) {
            pipeline.allocator.free(self.name);
        }
    }
};

const TransformRuntime = struct {
    queue: config_mod.QueueConfig,
    shards: []TransformShardRuntime,
    sharding: config_mod.SqlShardingConfig,
    workers_per_shard: usize,
    config: *const config_mod.TransformNode,
    id: []const u8,
    limits: sql_mod.runtime.LimitConfig,
    error_policy: sql_mod.runtime.ErrorPolicy,
    hash_seed: u64,
    shard_field: ?[]const u8,
    shard_metadata: ?config_mod.SqlShardMetadataKey,

    fn init(pipeline: *Pipeline, config: *const config_mod.TransformNode) Error!TransformRuntime {
        const exec = config.executionSettings();
        const sql_cfg = switch (config.*) {
            .sql => |value| value,
        };

        const shard_count = sql_cfg.sharding.shard_count;
        const workers_per_shard = exec.parallelism;

        var shards = try pipeline.allocator.alloc(TransformShardRuntime, shard_count);
        var shards_initialized: usize = 0;
        errdefer {
            var idx: usize = 0;
            while (idx < shards_initialized) : (idx += 1) {
                shards[idx].deinit(pipeline);
            }
            pipeline.allocator.free(shards);
        }

        var eviction = sql_mod.runtime.EvictionConfig{};
        if (sql_cfg.eviction.ttl_seconds) |ttl_secs| {
            const ttl_u64: u64 = @intCast(ttl_secs);
            eviction.ttl_ns = if (ttl_u64 == 0) null else secondsToNanoseconds(ttl_u64);
        }
        if (sql_cfg.eviction.max_groups) |max| {
            eviction.max_groups = max;
        }
        if (sql_cfg.eviction.sweep_interval_seconds) |sweep_secs| {
            const sweep_u64: u64 = @intCast(sweep_secs);
            eviction.sweep_interval_ns = secondsToNanoseconds(sweep_u64);
        }

        const limit_config = sql_mod.runtime.LimitConfig{
            .max_state_bytes = sql_cfg.limits.max_state_bytes,
            .max_row_bytes = sql_cfg.limits.max_row_bytes,
            .max_group_bytes = sql_cfg.limits.max_group_bytes,
            .cpu_budget_ns_per_second = sql_cfg.limits.cpu_budget_ns_per_second,
            .late_event_threshold_ns = if (sql_cfg.limits.late_event_threshold_seconds) |secs|
                secondsToNanoseconds(secs)
            else
                null,
        };

        const runtime_policy: sql_mod.runtime.ErrorPolicy = switch (sql_cfg.error_policy) {
            .skip_event => .skip_event,
            .null => .null,
            .clamp => .clamp,
            .propagate => .propagate,
        };

        var shard_index: usize = 0;
        while (shard_index < shard_count) : (shard_index += 1) {
            const name_info = try TransformRuntime.makeShardName(pipeline.allocator, config.id(), shard_index, shard_count);
            const shard_name = name_info.value;
            var shard_name_owned = name_info.owned;
            errdefer if (shard_name_owned) pipeline.allocator.free(shard_name);

            var channel_metrics = try pipeline.metrics.createChannelMetrics(
                pipeline.allocator,
                .{ .name = shard_name, .kind = .transform },
            );
            var channel_metrics_owned = true;
            errdefer if (channel_metrics_owned) channel_metrics.deinit();

            var channel = try EventChannel.init(pipeline.worker_allocator, exec.queue, null);
            var channel_owned = true;
            errdefer if (channel_owned) channel.deinit();

            var arena = try pipeline.allocator.create(std.heap.ArenaAllocator);
            errdefer pipeline.allocator.destroy(arena);
            arena.* = std.heap.ArenaAllocator.init(pipeline.allocator);
            var arena_initialized = true;
            errdefer if (arena_initialized) arena.deinit();

            const stmt = try sql_mod.parser.parseSelect(arena.allocator(), sql_cfg.query);

            var program = try sql_mod.runtime.compile(pipeline.worker_allocator, stmt, .{
                .eviction = eviction,
                .limits = limit_config,
                .error_policy = runtime_policy,
            });
            var program_owned = true;
            errdefer if (program_owned) program.deinit();

            const workers = try pipeline.allocator.alloc(TransformWorker, workers_per_shard);
            var workers_owned = true;
            errdefer if (workers_owned) pipeline.allocator.free(workers);
            for (workers) |*worker| worker.* = .{};

            shards[shard_index] = .{
                .name = shard_name,
                .name_owned = shard_name_owned,
                .channel = channel,
                .channel_metrics = channel_metrics,
                .batch_capacity = computeBatchCapacity(exec.queue),
                .arena = arena,
                .program = program,
                .program_mutex = .{},
                .workers = workers,
            };

            shard_name_owned = false;
            channel_owned = false;
            channel_metrics_owned = false;
            arena_initialized = false;
            program_owned = false;
            workers_owned = false;
            shards_initialized += 1;
        }

        var shard_field: ?[]const u8 = null;
        var shard_metadata: ?config_mod.SqlShardMetadataKey = null;
        if (sql_cfg.sharding.key) |key| {
            switch (key) {
                .field => |name| shard_field = name,
                .metadata => |meta| shard_metadata = meta,
            }
        }

        const runtime = TransformRuntime{
            .queue = exec.queue,
            .shards = shards,
            .sharding = sql_cfg.sharding,
            .workers_per_shard = workers_per_shard,
            .config = config,
            .id = config.id(),
            .limits = limit_config,
            .error_policy = runtime_policy,
            .hash_seed = wyhash.hash(0, config.id()),
            .shard_field = shard_field,
            .shard_metadata = shard_metadata,
        };
        return runtime;
    }

    fn spawn(self: *TransformRuntime, pipeline: *Pipeline, node_index: usize) Error!void {
        for (self.shards, 0..) |*shard, shard_index| {
            for (shard.workers) |*worker| {
                const context = try pipeline.allocator.create(TransformWorkerContext);
                errdefer pipeline.allocator.destroy(context);
                context.* = try TransformWorkerContext.init(pipeline, self, shard, shard_index, node_index);
                errdefer context.deinit();
                worker.context = context;
                worker.thread = try std.Thread.spawn(.{}, transformWorkerMain, .{context});
            }
        }
    }

    fn enqueue(self: *TransformRuntime, pipeline: *Pipeline, message: *EventMessage) void {
        var local = message.*;
        const selection = self.selectShard(local.event);
        if (selection.index) |shard_index| {
            if (selection.used_fallback) {
                pipeline.metrics.recordTransformError(self.id, "missing_shard_key_route", 1);
            }
            const shard = &self.shards[shard_index];
            retryPushEvent(&shard.channel, pipeline, &local);
        } else {
            pipeline.metrics.recordTransformError(self.id, "missing_shard_key_drop", 1);
            local.context.releaseSuccess();
        }
    }

    fn stop(self: *TransformRuntime) void {
        for (self.shards) |*shard| {
            shard.channel.clearObserver();
            shard.channel.close();
        }
    }

    fn join(self: *TransformRuntime, pipeline: *Pipeline) void {
        for (self.shards) |*shard| {
            for (shard.workers) |*worker| {
                if (worker.thread) |thread| thread.join();
                worker.thread = null;
                if (worker.context) |context| {
                    context.deinit();
                    pipeline.allocator.destroy(context);
                    worker.context = null;
                }
            }
        }
    }

    fn deinit(self: *TransformRuntime, pipeline: *Pipeline) void {
        for (self.shards) |*shard| {
            shard.deinit(pipeline);
        }
        pipeline.allocator.free(self.shards);
    }

    fn attachObserver(self: *TransformRuntime) void {
        for (self.shards) |*shard| {
            shard.channel.setObserver(shard.channel_metrics.observer());
        }
    }

    const ShardSelection = struct {
        index: ?usize,
        used_fallback: bool,
    };

    fn selectShard(self: *TransformRuntime, event: *const event_mod.Event) ShardSelection {
        const shard_total = self.shards.len;
        if (shard_total <= 1) return ShardSelection{ .index = 0, .used_fallback = false };

        var storage: [16]u8 = undefined;
        var key_bytes: []const u8 = &[_]u8{};
        var have_key = false;

        if (self.shard_field) |field_name| {
            if (event.payload == .log) {
                const log_event = event.payload.log;
                for (log_event.fields) |field| {
                    if (!std.mem.eql(u8, field.name, field_name)) continue;
                    switch (field.value) {
                        .string => |value| {
                            key_bytes = value;
                            have_key = true;
                        },
                        .integer => |value| {
                            std.mem.writeInt(i64, storage[0..8], value, .little);
                            key_bytes = storage[0..8];
                            have_key = true;
                        },
                        .float => |value| {
                            const bits = @as(u64, @bitCast(value));
                            std.mem.writeInt(u64, storage[0..8], bits, .little);
                            key_bytes = storage[0..8];
                            have_key = true;
                        },
                        .boolean => |value| {
                            storage[0] = if (value) 1 else 0;
                            key_bytes = storage[0..1];
                            have_key = true;
                        },
                        .null => {},
                    }
                    break;
                }
            }
        } else if (self.shard_metadata) |meta| {
            switch (meta) {
                .source_id => {
                    if (event.metadata.source_id) |source_id| {
                        key_bytes = source_id;
                        have_key = true;
                    }
                },
            }
        } else {
            if (event.metadata.source_id) |source_id| {
                key_bytes = source_id;
                have_key = true;
            }
        }

        if (!have_key) {
            return self.handleMissingShardKey();
        }

        const hash = wyhash.hash(self.hash_seed, key_bytes);
        const shard_index: usize = @intCast(hash % shard_total);
        return ShardSelection{ .index = shard_index, .used_fallback = false };
    }

    fn handleMissingShardKey(self: *TransformRuntime) ShardSelection {
        return switch (self.sharding.fallback) {
            .route_first => ShardSelection{
                .index = if (self.shards.len != 0) 0 else null,
                .used_fallback = true,
            },
            .drop => ShardSelection{ .index = null, .used_fallback = true },
        };
    }

    const ShardName = struct {
        value: []const u8,
        owned: bool,
    };

    fn makeShardName(
        allocator: std.mem.Allocator,
        base: []const u8,
        index: usize,
        total: usize,
    ) !ShardName {
        if (total == 1) {
            return ShardName{ .value = base, .owned = false };
        }
        const name = try std.fmt.allocPrint(allocator, "{s}_shard_{d}", .{ base, index });
        return ShardName{ .value = name, .owned = true };
    }
};

const TransformWorker = struct {
    thread: ?std.Thread = null,
    context: ?*TransformWorkerContext = null,
};

const TransformWorkerContext = struct {
    pipeline: *Pipeline,
    runtime: *TransformRuntime,
    shard: *TransformShardRuntime,
    shard_index: usize,
    node_index: usize,
    batch_buffer: []EventMessage,

    fn init(
        pipeline: *Pipeline,
        runtime: *TransformRuntime,
        shard: *TransformShardRuntime,
        shard_index: usize,
        node_index: usize,
    ) !TransformWorkerContext {
        const buffer = try pipeline.allocator.alloc(EventMessage, shard.batch_capacity);
        return TransformWorkerContext{
            .pipeline = pipeline,
            .runtime = runtime,
            .shard = shard,
            .shard_index = shard_index,
            .node_index = node_index,
            .batch_buffer = buffer,
        };
    }

    fn deinit(self: *TransformWorkerContext) void {
        self.pipeline.allocator.free(self.batch_buffer);
    }
};

fn transformWorkerMain(context: *TransformWorkerContext) void {
    while (true) {
        const count = context.shard.channel.popBatch(context.batch_buffer);
        if (count == 0) break;
        for (context.batch_buffer[0..count]) |message| {
            handleTransformMessage(context, message);
        }
    }
}

fn handleTransformMessage(context: *TransformWorkerContext, message: EventMessage) void {
    const pipeline = context.pipeline;
    const runtime = context.runtime;
    const shard = context.shard;
    const node_index = context.node_index;

    var produced: ?sql_mod.runtime.Row = null;
    var exec_error: ?sql_mod.runtime.Error = null;
    var executed = false;
    const OptionalEvictionStats = @TypeOf(shard.program.takeEvictions());
    const RowAdjustment = @TypeOf(shard.program.takeRowAdjustment());
    var eviction_stats: OptionalEvictionStats = null;
    var state_bytes: usize = 0;
    var group_count: usize = 0;
    var row_adjustment: RowAdjustment = .none;
    var metrics_ready = false;
    var locked_for_execution = false;

    if (runtime.limits.late_event_threshold_ns) |threshold_ns| {
        if (message.event.metadata.received_at) |received_at| {
            const now_wall = std.time.nanoTimestamp();
            var age = now_wall - received_at;
            if (age < 0) age = 0;
            const threshold_i128: i128 = @intCast(threshold_ns);
            if (age > threshold_i128) {
                exec_error = sql_mod.runtime.Error.LateEvent;
            }
        }
    }

    var duration_ns: u64 = 0;

    if (exec_error == null) execution: {
        locked_for_execution = true;
        shard.program_mutex.lock();
        defer shard.program_mutex.unlock();
        defer {
            if (executed) {
                eviction_stats = shard.program.takeEvictions();
                state_bytes = shard.program.stateBytes();
                group_count = shard.program.groupCount();
                metrics_ready = true;
            }
            row_adjustment = shard.program.takeRowAdjustment();
        }

        const now_ns = MonotonicClock.now();
        const exec_start = std.time.nanoTimestamp();
        executed = true;
        produced = shard.program.executeTracked(
            message.context.batchAllocator(),
            message.event,
            now_ns,
            exec_start,
            &duration_ns,
        ) catch |err| {
            exec_error = err;
            produced = null;
            break :execution;
        };
    }

    if (metrics_ready) {
        if (eviction_stats) |stats| {
            if (stats.ttl != 0) {
                const delta: u64 = @intCast(stats.ttl);
                pipeline.metrics.recordTransformEviction(shard.name, "ttl", delta);
            }
            if (stats.max_groups != 0) {
                const delta: u64 = @intCast(stats.max_groups);
                pipeline.metrics.recordTransformEviction(shard.name, "max_groups", delta);
            }
            if (stats.state_bytes != 0) {
                const delta: u64 = @intCast(stats.state_bytes);
                pipeline.metrics.recordTransformEviction(shard.name, "state_bytes", delta);
            }
        }
        pipeline.metrics.recordTransformState(shard.name, state_bytes);
        pipeline.metrics.recordTransformGroupCount(shard.name, group_count);
    }

    if (!locked_for_execution) {
        shard.program_mutex.lock();
        defer shard.program_mutex.unlock();
        row_adjustment = shard.program.takeRowAdjustment();
    }

    switch (row_adjustment) {
        .none => {},
        .clamped => {
            pipeline.metrics.recordTransformTruncate(shard.name, "row", 1);
            pipeline.metrics.recordTransformError(shard.name, "clamp", 1);
        },
        .nullified => {
            pipeline.metrics.recordTransformError(shard.name, "null", 1);
        },
    }

    pipeline.metrics.recordTransformProcessed(shard.name, 1);
    if (duration_ns != 0) {
        pipeline.metrics.recordTransformExecTime(shard.name, duration_ns);
    }

    if (exec_error) |err| {
        if (isLimitViolation(err)) {
            handleLimitViolation(runtime, shard, pipeline, &message, err);
        } else {
            message.context.releaseFailure();
        }
        return;
    }

    if (produced) |row| {
        var owned_row = row;
        const has_transforms = pipeline.hasDownstreamTransform(node_index);
        if (has_transforms) {
            const new_event = rowToEvent(
                message.context.batchAllocator(),
                &owned_row,
                message.event,
            ) catch {
                owned_row.deinit();
                message.context.releaseFailure();
                return;
            };
            pipeline.forwardEventToTransforms(node_index, new_event, message.context);
        }

        pipeline.sendRowFromTransform(node_index, owned_row, message.context) catch {
            message.context.releaseFailure();
            return;
        };
    }

    message.context.releaseSuccess();
}

fn isLimitViolation(err: sql_mod.runtime.Error) bool {
    return switch (err) {
        sql_mod.runtime.Error.RowTooLarge,
        sql_mod.runtime.Error.GroupStateTooLarge,
        sql_mod.runtime.Error.StateBudgetExceeded,
        sql_mod.runtime.Error.CpuBudgetExceeded,
        sql_mod.runtime.Error.LateEvent => true,
        else => false,
    };
}

fn handleLimitViolation(
    runtime: *TransformRuntime,
    shard: *TransformShardRuntime,
    pipeline: *Pipeline,
    message: *const EventMessage,
    violation: sql_mod.runtime.Error,
) void {
    if (violation == sql_mod.runtime.Error.LateEvent) {
        pipeline.metrics.recordTransformLateEvent(shard.name, 1);
    }

    const policy_label = switch (runtime.error_policy) {
        .skip_event => "skip_event",
        .null => "null",
        .clamp => "clamp",
        .propagate => "error",
    };
    pipeline.metrics.recordTransformError(shard.name, policy_label, 1);

    switch (runtime.error_policy) {
        .skip_event, .null, .clamp => {
            message.context.releaseSuccess();
        },
        .propagate => {
            message.context.releaseFailure();
        },
    }
}
const SinkRuntime = struct {
    channel: SinkChannel,
    queue: config_mod.QueueConfig,
    workers: []SinkWorker,
    batch_capacity: usize,
    config: *const config_mod.SinkNode,
    channel_metrics: metrics_mod.ChannelMetrics,
    id: []const u8,

    fn init(pipeline: *Pipeline, config: *const config_mod.SinkNode) Error!SinkRuntime {
        const exec = config.executionSettings();
        var channel_metrics = try pipeline.metrics.createChannelMetrics(
            pipeline.allocator,
            .{ .name = config.id(), .kind = .sink },
        );
        var channel_metrics_owned = true;
        errdefer if (channel_metrics_owned) channel_metrics.deinit();

        var channel = try SinkChannel.init(pipeline.worker_allocator, exec.queue, null);
        var channel_owned = true;
        errdefer if (channel_owned) channel.deinit();

        const workers = try pipeline.allocator.alloc(SinkWorker, exec.parallelism);
        errdefer pipeline.allocator.free(workers);
        for (workers) |*worker| worker.* = .{};

        const runtime = SinkRuntime{
            .channel = channel,
            .queue = exec.queue,
            .workers = workers,
            .batch_capacity = computeBatchCapacity(exec.queue),
            .config = config,
            .channel_metrics = channel_metrics,
            .id = config.id(),
        };
        channel_owned = false;
        channel_metrics_owned = false;
        return runtime;
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
        self.channel.clearObserver();
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
        self.channel.drainWith(struct {
            fn release(message: SinkMessage) void {
                message.row.deinit();
                message.context.releaseFailure();
            }
        }.release);
        self.channel.deinit();
        pipeline.allocator.free(self.workers);
        self.channel_metrics.deinit();
    }

    fn attachObserver(self: *SinkRuntime) void {
        self.channel.setObserver(self.channel_metrics.observer());
    }
};

const SinkWorker = struct {
    thread: ?std.Thread = null,
    context: ?*SinkWorkerContext = null,
};

const SinkWorkerContext = struct {
    pipeline: *Pipeline,
    node_index: usize,
    node_name: []const u8,
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
            .node_name = config.id(),
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
            context.pipeline.metrics.recordSinkEmitted(runtime.id, 1);
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
    source_name: []const u8,

    fn create(
        allocator: std.mem.Allocator,
        metrics: *const metrics_mod.PipelineMetrics,
        ack: event_mod.AckToken,
        total_events: usize,
        source_name: []const u8,
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
            .source_name = source_name,
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
            self.metrics.recordPipelineLatency(self.source_name, latency_u64);
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
    var copied: usize = 0;
    var success = false;
    errdefer {
        if (!success) {
            if (source.owns_strings) {
                var idx = copied;
                while (idx > 0) {
                    idx -= 1;
                    const entry = entries[idx];
                    if (entry.value == .string) {
                        allocator.free(entry.value.string);
                    }
                }
            }
            allocator.free(entries);
        }
    }

    while (copied < source.values.len) : (copied += 1) {
        var entry = source.values[copied];
        if (source.owns_strings and entry.value == .string) {
            const buffer = try copyBytes(allocator, entry.value.string);
            entry.value = .{ .string = buffer };
        }
        entries[copied] = entry;
    }

    success = true;
    return sql_mod.runtime.Row{
        .allocator = allocator,
        .values = entries,
        .owns_strings = source.owns_strings,
    };
}

fn copyBytes(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const buffer = try allocator.alloc(u8, input.len);
    if (input.len != 0) {
        std.mem.copyForwards(u8, buffer, input);
    }
    return buffer;
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

fn rowToEvent(
    allocator: std.mem.Allocator,
    row: *const sql_mod.runtime.Row,
    template: *const event_mod.Event,
) !*event_mod.Event {
    const ascii = std.ascii;

    var message = switch (template.payload) {
        .log => |log_event| log_event.message,
    };

    var field_total: usize = 0;
    for (row.values) |entry| {
        const is_message = ascii.eqlIgnoreCase(entry.name, "message");
        if (is_message and entry.value == .string) {
            message = entry.value.string;
            continue;
        }
        field_total += 1;
    }

    const fields = try allocator.alloc(event_mod.Field, field_total);
    var populated: usize = 0;
    errdefer {
        var idx = populated;
        while (idx > 0) {
            idx -= 1;
            const field = fields[idx];
            allocator.free(field.name);
            if (field.value == .string) {
                allocator.free(field.value.string);
            }
        }
        allocator.free(fields);
    }

    var owned_message: ?[]u8 = null;
    errdefer if (owned_message) |buffer| allocator.free(buffer);

    var field_index: usize = 0;
    for (row.values) |entry| {
        const is_message = ascii.eqlIgnoreCase(entry.name, "message");
        if (is_message and entry.value == .string) {
            if (owned_message) |buffer| {
                allocator.free(buffer);
                owned_message = null;
            }
            owned_message = try copyBytes(allocator, entry.value.string);
            message = owned_message.?;
            continue;
        }
        if (field_index < fields.len) {
            const name_copy = try copyBytes(allocator, entry.name);

            var value = entry.value;
            if (value == .string) {
                const value_copy = try copyBytes(allocator, value.string);
                value = .{ .string = value_copy };
            }

            fields[field_index] = .{ .name = name_copy, .value = value };
            populated = field_index + 1;
            field_index += 1;
        }
    }

    const metadata = template.metadata;

    const event_ptr = try allocator.create(event_mod.Event);
    errdefer allocator.destroy(event_ptr);

    event_ptr.* = .{
        .metadata = metadata,
        .payload = .{
            .log = .{
                .message = owned_message orelse message,
                .fields = fields,
            },
        },
    };
    owned_message = null;
    populated = 0;

    return event_ptr;
}

test "rowToEvent clones row data" {
    const allocator = testing.allocator;

    var entries = try allocator.alloc(sql_mod.runtime.ValueEntry, 2);
    var row = sql_mod.runtime.Row{ .allocator = allocator, .values = entries };
    var row_deinited = false;
    defer if (!row_deinited) row.deinit();

    entries[0] = .{ .name = "message", .value = .{ .string = "overridden" } };
    entries[1] = .{ .name = "custom", .value = .{ .string = "payload" } };

    const template = event_mod.Event{
        .metadata = .{},
        .payload = .{ .log = .{ .message = "original", .fields = &[_]event_mod.Field{} } },
    };

    const event_ptr = try rowToEvent(allocator, &row, &template);
    defer {
        const payload = event_ptr.payload.log;
        allocator.free(payload.message);
        for (payload.fields) |field| {
            allocator.free(field.name);
            if (field.value == .string) {
                allocator.free(field.value.string);
            }
        }
        allocator.free(payload.fields);
        allocator.destroy(event_ptr);
    }

    row.deinit();
    row_deinited = true;

    const payload = event_ptr.payload.log;
    try testing.expectEqualStrings("overridden", payload.message);
    try testing.expectEqualStrings("custom", payload.fields[0].name);
    try testing.expect(payload.fields[0].value == .string);
    try testing.expectEqualStrings("payload", payload.fields[0].value.string);
}

test "cloneRow deep copies string values" {
    const allocator = testing.allocator;

    var entries = try allocator.alloc(sql_mod.runtime.ValueEntry, 1);
    entries[0] = .{ .name = "message", .value = .{ .string = try copyBytes(allocator, "hello") } };

    var original = sql_mod.runtime.Row{
        .allocator = allocator,
        .values = entries,
        .owns_strings = true,
    };
    var original_deinited = false;
    defer if (!original_deinited) original.deinit();

    const original_ptr = original.values[0].value.string.ptr;

    var clone = try cloneRow(allocator, original);
    defer clone.deinit();

    try testing.expect(clone.owns_strings);
    try testing.expect(clone.values[0].value == .string);
    try testing.expect(clone.values[0].value.string.ptr != original_ptr);

    original.deinit();
    original_deinited = true;

    try testing.expectEqualStrings("hello", clone.values[0].value.string);
}

fn defaultSinkBuilder(allocator: std.mem.Allocator, node: config_mod.SinkNode) sink_mod.Error!sink_mod.Sink {
    return switch (node) {
        .console => |cfg| console_sink.build(allocator, cfg),
    };
}

const testing = std.testing;

const TestSink = struct {
    const Context = struct {
        allocator: std.mem.Allocator,
        mutex: std.Thread.Mutex = .{},
        seen: bool = false,
        count: ?i64 = null,
        message: ?[]const u8 = null,
        message_storage: ?[]u8 = null,
    };

    const Snapshot = struct {
        seen: bool,
        count: ?i64,
        message: ?[]const u8,
    };

    var shared_context: ?*Context = null;

    fn builder(allocator: std.mem.Allocator, _: config_mod.SinkNode) sink_mod.Error!sink_mod.Sink {
        const ctx = try allocator.create(Context);
        ctx.* = .{
            .allocator = allocator,
            .mutex = .{},
            .seen = false,
            .count = null,
            .message = null,
            .message_storage = null,
        };
        shared_context = ctx;
        return sink_mod.Sink{ .context = ctx, .vtable = &vtable };
    }

    fn emit(context: *anyopaque, row: *const sql_mod.runtime.Row) sink_mod.Error!void {
        const ctx: *Context = @ptrCast(@alignCast(context));
        ctx.mutex.lock();
        defer ctx.mutex.unlock();
        ctx.seen = true;
        if (row.values.len > 0 and row.values[0].value == .string) {
            if (ctx.message_storage) |existing| {
                ctx.allocator.free(existing);
                ctx.message_storage = null;
                ctx.message = null;
            }
            const slice = row.values[0].value.string;
            const copy = try ctx.allocator.alloc(u8, slice.len);
            if (slice.len != 0) std.mem.copyForwards(u8, copy, slice);
            ctx.message_storage = copy;
            ctx.message = copy;
        }
        if (row.values.len > 1 and row.values[1].value == .integer) {
            ctx.count = row.values[1].value.integer;
        }
    }

    fn flush(_: *anyopaque) sink_mod.Error!void {
        return;
    }

    fn deinit(context: *anyopaque, allocator: std.mem.Allocator) void {
        const ctx: *Context = @ptrCast(@alignCast(context));
        if (ctx.message_storage) |message| {
            ctx.allocator.free(message);
        }
        allocator.destroy(ctx);
    }

    fn snapshot() Snapshot {
        const ctx = shared_context orelse return Snapshot{ .seen = false, .count = null, .message = null };
        ctx.mutex.lock();
        const snap = Snapshot{ .seen = ctx.seen, .count = ctx.count, .message = ctx.message };
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

        state.fields = ctx.allocator.alloc(event_mod.Field, 1) catch return source_mod.SourceError.StartupFailed;
        errdefer ctx.allocator.free(state.fields);
        state.fields[0] = .{ .name = "syslog_severity", .value = .{ .integer = 4 } };

        state.event_storage[0] = event_mod.Event{
            .metadata = .{ .source_id = config.id },
            .payload = .{ .log = .{ .message = "hello", .fields = state.fields } },
        };

        const lifecycle = source_mod.BatchLifecycle{
            .poll_batch = pollBatch,
            .shutdown = shutdown,
        };

        return source_mod.Source{
            .batch = .{
                .descriptor = .{ .type = .syslog, .name = config.id },
                .capabilities = .{ .streaming = false, .batching = true },
                .context = state,
                .lifecycle = lifecycle,
            },
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
                .query = "SELECT message, COUNT(*) AS total FROM logs GROUP BY message",
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
            try testing.expectEqual(@as(?i64, 1), snapshot.count);
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

test "pipeline composes sql transforms" {
    TestSink.reset();
    const allocator = testing.allocator;

    const source_config = source_cfg.SourceConfig{
        .id = "test_source",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:0" } },
    };

    const pipeline_cfg = config_mod.PipelineConfig{
        .sources = &[_]config_mod.SourceNode{
            .{ .id = "test_source", .config = source_config, .outputs = &[_][]const u8{"sql_primary"} },
        },
        .transforms = &[_]config_mod.TransformNode{
            .{ .sql = .{
                .id = "sql_primary",
                .inputs = &[_][]const u8{"test_source"},
                .outputs = &[_][]const u8{"sql_secondary"},
                .query = "SELECT message, COUNT(*) AS total FROM logs GROUP BY message",
            } },
            .{ .sql = .{
                .id = "sql_secondary",
                .inputs = &[_][]const u8{"sql_primary"},
                .outputs = &[_][]const u8{"sink"},
                .query = "SELECT message, SUM(total) AS total_events FROM logs GROUP BY message",
            } },
        },
        .sinks = &[_]config_mod.SinkNode{
            .{ .console = .{ .id = "sink", .inputs = &[_][]const u8{"sql_secondary"} } },
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
            try testing.expectEqual(@as(?i64, 1), snapshot.count);
            try testing.expectEqualStrings("hello", snapshot.message orelse "");
            break;
        }
        std.Thread.sleep(1 * std.time.ns_per_ms);
    } else {
        try testing.expect(false);
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

test "transform sharding uses metadata source id" {
    const allocator = testing.allocator;

    const source_config = source_cfg.SourceConfig{
        .id = "meta_source",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:0" } },
    };

    const transform = config_mod.SqlTransform{
        .id = "sql_meta",
        .inputs = &[_][]const u8{"meta_source"},
        .outputs = &[_][]const u8{"sink"},
        .query = "SELECT message, COUNT(*) AS total FROM logs GROUP BY message",
        .sharding = .{
            .shard_count = 4,
            .key = .{ .metadata = .source_id },
        },
    };

    const pipeline_cfg = config_mod.PipelineConfig{
        .sources = &[_]config_mod.SourceNode{
            .{ .id = "meta_source", .config = source_config, .outputs = &[_][]const u8{"sql_meta"} },
        },
        .transforms = &[_]config_mod.TransformNode{.{ .sql = transform }},
        .sinks = &[_]config_mod.SinkNode{
            .{ .console = .{ .id = "sink", .inputs = &[_][]const u8{"sql_meta"} } },
        },
    };

    const registry = source_mod.Registry{ .factories = &[_]source_mod.SourceFactory{TestSource.factory()} };

    var pipeline = try Pipeline.init(allocator, &pipeline_cfg, .{ .collector = .{ .registry = registry } });
    defer pipeline.deinit();

    var transform_index: ?usize = null;
    for (pipeline.graph.nodes, 0..) |node, idx| {
        if (node.kind == .transform) {
            transform_index = idx;
            break;
        }
    }
    const idx = transform_index orelse unreachable;
    const runtime = &pipeline.nodes[idx].data.transform;

    const log_fields = [_]event_mod.Field{};
    const event = event_mod.Event{
        .metadata = .{ .source_id = "meta_source" },
        .payload = .{ .log = .{ .message = "hello", .fields = &log_fields } },
    };

    const selection = runtime.selectShard(&event);
    try testing.expect(!selection.used_fallback);
    const shard_index = selection.index orelse unreachable;
    try testing.expect(shard_index < runtime.shards.len);
}

test "transform sharding drops missing key when configured" {
    const allocator = testing.allocator;

    const source_config = source_cfg.SourceConfig{
        .id = "drop_source",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:0" } },
    };

    const transform = config_mod.SqlTransform{
        .id = "sql_drop",
        .inputs = &[_][]const u8{"drop_source"},
        .outputs = &[_][]const u8{"sink"},
        .query = "SELECT message, COUNT(*) AS total FROM logs GROUP BY message",
        .sharding = .{
            .shard_count = 2,
            .key = .{ .field = "hostname" },
            .fallback = .drop,
        },
    };

    const pipeline_cfg = config_mod.PipelineConfig{
        .sources = &[_]config_mod.SourceNode{
            .{ .id = "drop_source", .config = source_config, .outputs = &[_][]const u8{"sql_drop"} },
        },
        .transforms = &[_]config_mod.TransformNode{.{ .sql = transform }},
        .sinks = &[_]config_mod.SinkNode{
            .{ .console = .{ .id = "sink", .inputs = &[_][]const u8{"sql_drop"} } },
        },
    };

    const registry = source_mod.Registry{ .factories = &[_]source_mod.SourceFactory{TestSource.factory()} };

    var pipeline = try Pipeline.init(allocator, &pipeline_cfg, .{ .collector = .{ .registry = registry } });
    defer pipeline.deinit();

    var transform_index: ?usize = null;
    for (pipeline.graph.nodes, 0..) |node, idx| {
        if (node.kind == .transform) {
            transform_index = idx;
            break;
        }
    }
    const idx = transform_index orelse unreachable;
    const runtime = &pipeline.nodes[idx].data.transform;

    const no_hostname_fields = [_]event_mod.Field{};
    const event_missing = event_mod.Event{
        .metadata = .{ .source_id = "drop_source" },
        .payload = .{ .log = .{ .message = "hello", .fields = &no_hostname_fields } },
    };

    const selection_missing = runtime.selectShard(&event_missing);
    try testing.expect(selection_missing.used_fallback);
    try testing.expect(selection_missing.index == null);

    const hostname_fields = [_]event_mod.Field{
        .{ .name = "hostname", .value = .{ .string = "node-a" } },
    };
    const event_present = event_mod.Event{
        .metadata = .{ .source_id = "drop_source" },
        .payload = .{ .log = .{ .message = "hello", .fields = &hostname_fields } },
    };

    const selection_present = runtime.selectShard(&event_present);
    try testing.expect(!selection_present.used_fallback);
    const shard_index = selection_present.index orelse unreachable;
    try testing.expect(shard_index < runtime.shards.len);
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

    var batch_ctx = try BatchContext.create(pipeline.worker_allocator, &pipeline.metrics, token, 1, "test");
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

    var batch_ctx = try BatchContext.create(allocator, &pipeline.metrics, token, 2, "test");
    errdefer batch_ctx.forceRelease();

    const ctx_old = try EventContext.create(allocator, batch_ctx);
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

test "retryPushEvent releases evicted context" {
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

    var recorder_alive = true;
    const Recorder = struct {
        calls: usize = 0,
        status: ?event_mod.AckStatus = null,
        alive_flag: *bool,

        fn complete(context: *anyopaque, status: event_mod.AckStatus) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.calls += 1;
            self.status = status;
            self.alive_flag.* = false;
        }
    };

    var recorder = Recorder{ .alive_flag = &recorder_alive };
    var handle = event_mod.AckHandle.init(&recorder, Recorder.complete);
    const token = event_mod.AckToken.init(&handle);

    var batch_ctx = try BatchContext.create(allocator, &pipeline.metrics, token, 1, "test");
    defer if (recorder_alive) batch_ctx.forceRelease();

    const ctx_old = try EventContext.create(allocator, batch_ctx);

    const event = event_mod.Event{
        .metadata = .{},
        .payload = .{ .log = .{ .message = "drop", .fields = &[_]event_mod.Field{} } },
    };

    var message_old = EventMessage{ .event = &event, .context = ctx_old };
    retryPushEvent(&channel, &pipeline, &message_old);

    var batch_ctx_new_alive = true;
    var batch_ctx_new = try BatchContext.create(allocator, &pipeline.metrics, event_mod.AckToken.none(), 1, "test");
    defer if (batch_ctx_new_alive) batch_ctx_new.forceRelease();

    const ctx_new = try EventContext.create(allocator, batch_ctx_new);
    var message_new = EventMessage{ .event = &event, .context = ctx_new };
    retryPushEvent(&channel, &pipeline, &message_new);

    try testing.expectEqual(@as(usize, 1), recorder.calls);
    const status = recorder.status orelse unreachable;
    try testing.expectEqual(event_mod.AckStatus.retryable_failure, status);

    const popped = channel.pop() orelse unreachable;
    try testing.expect(popped.context == ctx_new);
    popped.context.releaseSuccess();
    batch_ctx_new_alive = false;
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

    var success_ctx = try BatchContext.create(allocator, &pipeline_metrics, event_mod.AckToken.none(), 1, "test");
    std.Thread.sleep(1 * std.time.ns_per_ms);
    success_ctx.complete(true);

    var failure_ctx = try BatchContext.create(allocator, &pipeline_metrics, event_mod.AckToken.none(), 1, "test");
    std.Thread.sleep(1 * std.time.ns_per_ms);
    failure_ctx.complete(false);

    try testing.expectEqual(@as(u64, 1), recorder.success);
    try testing.expectEqual(@as(u64, 1), recorder.retryable);
    try testing.expectEqual(@as(u64, 0), recorder.permanent);
    try testing.expectEqual(@as(u64, 2), recorder.latency_count);
    try testing.expect(recorder.latency_total >= recorder.latency_count);
}
