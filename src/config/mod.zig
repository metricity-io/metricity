const std = @import("std");
const source_mod = @import("source");
const source_cfg = source_mod.config;

pub const ValidationError = error{
    DuplicateComponent,
    SourceIdMismatch,
    UnknownComponent,
    InvalidEdge,
    MissingInputs,
    MissingSinks,
    InvalidParallelism,
    InvalidQueueCapacity,
};

pub const OwnedPipelineConfig = struct {
    arena: *std.heap.ArenaAllocator,
    pipeline: PipelineConfig,

    pub fn deinit(self: OwnedPipelineConfig, allocator: std.mem.Allocator) void {
        self.arena.deinit();
        allocator.destroy(self.arena);
    }
};

pub const PipelineConfig = struct {
    sources: []const SourceNode,
    transforms: []const TransformNode,
    sinks: []const SinkNode,

    pub fn validate(self: PipelineConfig, allocator: std.mem.Allocator) (ValidationError || std.mem.Allocator.Error)!void {
        if (self.sinks.len == 0) return ValidationError.MissingSinks;

        var components = std.StringHashMap(ComponentKind).init(allocator);
        defer components.deinit();

        for (self.sources) |src| {
            if (!std.mem.eql(u8, src.config.id, src.id)) {
                return ValidationError.SourceIdMismatch;
            }
            const entry = try components.getOrPut(src.id);
            if (entry.found_existing) return ValidationError.DuplicateComponent;
            entry.value_ptr.* = .source;
        }

        for (self.transforms) |transform| {
            const id = transform.id();
            const entry = try components.getOrPut(id);
            if (entry.found_existing) return ValidationError.DuplicateComponent;
            entry.value_ptr.* = .transform;
        }

        for (self.sinks) |sink| {
            const id = sink.id();
            const entry = try components.getOrPut(id);
            if (entry.found_existing) return ValidationError.DuplicateComponent;
            entry.value_ptr.* = .sink;
        }

        for (self.sources) |src| {
            try validateEdges(.source, src.outputs, components);
        }

        for (self.transforms) |transform| {
            try validateInputs(transform.inputs(), components);
            try validateEdges(.transform, transform.outputs(), components);
            try validateExecutionSettings(transform.executionSettings());
        }

        for (self.sinks) |sink| {
            const inputs = sink.inputs();
            if (inputs.len == 0) return ValidationError.MissingInputs;
            try validateInputs(inputs, components);
            try validateExecutionSettings(sink.executionSettings());
        }
    }
};

pub const SourceNode = struct {
    id: []const u8,
    config: source_cfg.SourceConfig,
    outputs: []const []const u8 = &[_][]const u8{},
};

pub const TransformType = enum {
    sql,
};

pub const SqlTransform = struct {
    id: []const u8,
    inputs: []const []const u8,
    outputs: []const []const u8 = &[_][]const u8{},
    query: []const u8,
    parallelism: usize = 1,
    queue: QueueConfig = .{},
};

pub const TransformNode = union(TransformType) {
    sql: SqlTransform,

    pub fn id(self: TransformNode) []const u8 {
        return switch (self) {
            .sql => |t| t.id,
        };
    }

    pub fn inputs(self: TransformNode) []const []const u8 {
        return switch (self) {
            .sql => |t| t.inputs,
        };
    }

    pub fn outputs(self: TransformNode) []const []const u8 {
        return switch (self) {
            .sql => |t| t.outputs,
        };
    }

    pub fn executionSettings(self: TransformNode) ExecutionSettings {
        return switch (self) {
            .sql => |t| ExecutionSettings{ .parallelism = t.parallelism, .queue = t.queue },
        };
    }
};

pub const SinkType = enum {
    console,
};

pub const ConsoleTarget = enum {
    stdout,
    stderr,
};

pub const ConsoleSink = struct {
    id: []const u8,
    inputs: []const []const u8,
    target: ConsoleTarget = .stdout,
    parallelism: usize = 1,
    queue: QueueConfig = .{},
};

pub const SinkNode = union(SinkType) {
    console: ConsoleSink,

    pub fn id(self: SinkNode) []const u8 {
        return switch (self) {
            .console => |c| c.id,
        };
    }

    pub fn inputs(self: SinkNode) []const []const u8 {
        return switch (self) {
            .console => |c| c.inputs,
        };
    }

    pub fn executionSettings(self: SinkNode) ExecutionSettings {
        return switch (self) {
            .console => |c| ExecutionSettings{ .parallelism = c.parallelism, .queue = c.queue },
        };
    }
};

const ComponentKind = enum {
    source,
    transform,
    sink,
};

pub const QueueStrategy = enum { reject, drop_newest, drop_oldest };

pub const QueueConfig = struct {
    capacity: usize = 1024,
    strategy: QueueStrategy = .reject,
};

pub const ExecutionSettings = struct {
    parallelism: usize = 1,
    queue: QueueConfig = .{},
};

fn validateEdges(origin: ComponentKind, targets: []const []const u8, components: std.StringHashMap(ComponentKind)) ValidationError!void {
    for (targets) |target| {
        const kind = components.get(target) orelse return ValidationError.UnknownComponent;
        switch (origin) {
            .source => switch (kind) {
                .transform, .sink => {},
                .source => return ValidationError.InvalidEdge,
            },
            .transform => switch (kind) {
                .transform, .sink => {},
                .source => return ValidationError.InvalidEdge,
            },
            .sink => return ValidationError.InvalidEdge,
        }
    }
}

fn validateInputs(inputs: []const []const u8, components: std.StringHashMap(ComponentKind)) ValidationError!void {
    if (inputs.len == 0) return ValidationError.MissingInputs;
    for (inputs) |target| {
        const kind = components.get(target) orelse return ValidationError.UnknownComponent;
        switch (kind) {
            .source, .transform => {},
            .sink => return ValidationError.InvalidEdge,
        }
    }
}

fn validateExecutionSettings(settings: ExecutionSettings) ValidationError!void {
    if (settings.parallelism == 0) return ValidationError.InvalidParallelism;
    if (settings.queue.capacity == 0) return ValidationError.InvalidQueueCapacity;
}

const testing = std.testing;

test "validate simple pipeline" {
    const allocator = testing.allocator;
    const source_config = source_cfg.SourceConfig{
        .id = "in",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:514" } },
    };

    const pipeline = PipelineConfig{
        .sources = &[_]SourceNode{
            .{ .id = "in", .config = source_config, .outputs = &[_][]const u8{"sql"} },
        },
        .transforms = &[_]TransformNode{
            .{ .sql = .{ .id = "sql", .inputs = &[_][]const u8{"in"}, .outputs = &[_][]const u8{"out"}, .query = "SELECT * FROM logs" } },
        },
        .sinks = &[_]SinkNode{
            .{ .console = .{ .id = "out", .inputs = &[_][]const u8{"sql"} } },
        },
    };

    try pipeline.validate(allocator);
}

test "validate rejects zero parallelism" {
    const allocator = testing.allocator;
    const source_config = source_cfg.SourceConfig{
        .id = "in",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:514" } },
    };

    const pipeline = PipelineConfig{
        .sources = &[_]SourceNode{
            .{ .id = "in", .config = source_config, .outputs = &[_][]const u8{"sql"} },
        },
        .transforms = &[_]TransformNode{
            .{ .sql = .{
                .id = "sql",
                .inputs = &[_][]const u8{"in"},
                .outputs = &[_][]const u8{"out"},
                .query = "SELECT * FROM logs",
                .parallelism = 0,
            } },
        },
        .sinks = &[_]SinkNode{
            .{ .console = .{ .id = "out", .inputs = &[_][]const u8{"sql"} } },
        },
    };

    try testing.expectError(ValidationError.InvalidParallelism, pipeline.validate(allocator));
}

test "validate rejects zero queue capacity" {
    const allocator = testing.allocator;
    const source_config = source_cfg.SourceConfig{
        .id = "in",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:514" } },
    };

    const pipeline = PipelineConfig{
        .sources = &[_]SourceNode{
            .{ .id = "in", .config = source_config, .outputs = &[_][]const u8{"sql"} },
        },
        .transforms = &[_]TransformNode{
            .{ .sql = .{
                .id = "sql",
                .inputs = &[_][]const u8{"in"},
                .outputs = &[_][]const u8{"out"},
                .query = "SELECT * FROM logs",
                .queue = .{ .capacity = 0, .strategy = .reject },
            } },
        },
        .sinks = &[_]SinkNode{
            .{ .console = .{ .id = "out", .inputs = &[_][]const u8{"sql"} } },
        },
    };

    try testing.expectError(ValidationError.InvalidQueueCapacity, pipeline.validate(allocator));
}

test "detect missing sink" {
    const allocator = testing.allocator;
    const source_config = source_cfg.SourceConfig{
        .id = "in",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:514" } },
    };

    const pipeline = PipelineConfig{
        .sources = &[_]SourceNode{
            .{ .id = "in", .config = source_config, .outputs = &[_][]const u8{} },
        },
        .transforms = &[_]TransformNode{},
        .sinks = &[_]SinkNode{},
    };

    try testing.expectError(ValidationError.MissingSinks, pipeline.validate(allocator));
}

test "detect unknown target" {
    const allocator = testing.allocator;
    const source_config = source_cfg.SourceConfig{
        .id = "in",
        .payload = .{ .syslog = .{ .address = "udp://127.0.0.1:514" } },
    };

    const pipeline = PipelineConfig{
        .sources = &[_]SourceNode{
            .{ .id = "in", .config = source_config, .outputs = &[_][]const u8{"missing"} },
        },
        .transforms = &[_]TransformNode{},
        .sinks = &[_]SinkNode{
            .{ .console = .{ .id = "out", .inputs = &[_][]const u8{"in"} } },
        },
    };

    try testing.expectError(ValidationError.UnknownComponent, pipeline.validate(allocator));
}
