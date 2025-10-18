const std = @import("std");
const cfg = @import("mod.zig");
const source_mod = @import("source");
const source_cfg = source_mod.config;

const ascii = std.ascii;

pub const ParseError = error{
    InvalidSyntax,
    UnknownSection,
    DuplicateComponent,
    DuplicateKey,
    MissingType,
    UnsupportedTransformType,
    UnsupportedSinkType,
    MissingInputs,
    MissingQuery,
    InvalidValue,
    MissingSection,
    ConfigTooLarge,
};

pub const max_config_bytes: usize = 4 * 1024 * 1024;

const SectionKind = enum { sources, transforms, sinks };

const SectionRef = union(enum) {
    source: usize,
    transform: usize,
    transform_limits: usize,
    transform_sharding: usize,
    sink: usize,
};

const SourceDraft = struct {
    id: []const u8,
    entries: std.ArrayListUnmanaged(source_cfg.TomlKeyValue) = .{},
    outputs: ?[]const []const u8 = null,
};

const TransformDraft = struct {
    id: []const u8,
    kind: ?cfg.TransformType = null,
    inputs: ?[]const []const u8 = null,
    outputs: ?[]const []const u8 = null,
    query: ?[]const u8 = null,
    parallelism: ?usize = null,
    queue_capacity: ?usize = null,
    queue_strategy: ?cfg.QueueStrategy = null,
    eviction_ttl_seconds: ?usize = null,
    eviction_max_groups: ?usize = null,
    eviction_sweep_seconds: ?usize = null,
    limits_max_state_bytes: ?usize = null,
    limits_max_row_bytes: ?usize = null,
    limits_max_group_bytes: ?usize = null,
    limits_cpu_budget_ns_per_sec: ?u64 = null,
    limits_late_event_threshold_seconds: ?usize = null,
    error_policy: ?cfg.SqlErrorPolicy = null,
    sharding_shard_count: ?usize = null,
    sharding_key_field: ?[]const u8 = null,
    sharding_key_metadata: ?cfg.SqlShardMetadataKey = null,
    sharding_fallback: ?cfg.SqlShardFallback = null,
    event_time_field: ?[]const u8 = null,
    event_time_metadata: ?cfg.SqlEventTimeMetadata = null,
    watermark_lag_seconds: ?usize = null,
    allowed_lateness_seconds: ?usize = null,
};

const SinkDraft = struct {
    id: []const u8,
    kind: ?cfg.SinkType = null,
    inputs: ?[]const []const u8 = null,
    target: cfg.ConsoleTarget = .stdout,
    parallelism: ?usize = null,
    queue_capacity: ?usize = null,
    queue_strategy: ?cfg.QueueStrategy = null,
};

pub fn parseFile(allocator: std.mem.Allocator, path: []const u8) !cfg.OwnedPipelineConfig {
    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    const data = file.readToEndAlloc(allocator, max_config_bytes) catch |err| {
        return switch (err) {
            error.FileTooBig => ParseError.ConfigTooLarge,
            else => err,
        };
    };
    defer allocator.free(data);

    return parse(allocator, data);
}

pub fn parse(allocator: std.mem.Allocator, source: []const u8) !cfg.OwnedPipelineConfig {
    var arena_ptr = try allocator.create(std.heap.ArenaAllocator);
    errdefer allocator.destroy(arena_ptr);
    arena_ptr.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena_ptr.deinit();
    const arena = arena_ptr.allocator();

    var id_registry = std.StringHashMap(void).init(allocator);
    defer id_registry.deinit();

    var source_drafts = std.ArrayList(SourceDraft){};
    defer source_drafts.deinit(allocator);

    var transform_drafts = std.ArrayList(TransformDraft){};
    defer transform_drafts.deinit(allocator);

    var sink_drafts = std.ArrayList(SinkDraft){};
    defer sink_drafts.deinit(allocator);

    var lines = std.mem.tokenizeScalar(u8, source, '\n');
    var current: ?SectionRef = null;

    while (lines.next()) |raw_line| {
        const no_comment = stripComment(raw_line);
        const trimmed = std.mem.trim(u8, no_comment, " \t\r");
        if (trimmed.len == 0) continue;

        if (trimmed[0] == '[') {
            const section = try parseSection(arena, trimmed);
            if (section.subsection) |subsection| {
                switch (section.kind) {
                    .transforms => {
                        var existing_idx: ?usize = null;
                        var i: usize = 0;
                        while (i < transform_drafts.items.len) : (i += 1) {
                            if (std.mem.eql(u8, transform_drafts.items[i].id, section.id)) {
                                existing_idx = i;
                                break;
                            }
                        }
                        if (existing_idx == null) return ParseError.MissingSection;
                        if (std.mem.eql(u8, subsection, "limits")) {
                            current = SectionRef{ .transform_limits = existing_idx.? };
                        } else if (std.mem.eql(u8, subsection, "sharding")) {
                            current = SectionRef{ .transform_sharding = existing_idx.? };
                        } else {
                            return ParseError.UnknownSection;
                        }
                    },
                    else => return ParseError.InvalidSyntax,
                }
                continue;
            }

            try registerId(allocator, &id_registry, section.id);
            switch (section.kind) {
                .sources => {
                    const idx = source_drafts.items.len;
                    const draft = SourceDraft{ .id = section.id };
                    try source_drafts.append(allocator, draft);
                    current = SectionRef{ .source = idx };
                },
                .transforms => {
                    const idx = transform_drafts.items.len;
                    const draft = TransformDraft{ .id = section.id };
                    try transform_drafts.append(allocator, draft);
                    current = SectionRef{ .transform = idx };
                },
                .sinks => {
                    const idx = sink_drafts.items.len;
                    const draft = SinkDraft{ .id = section.id };
                    try sink_drafts.append(allocator, draft);
                    current = SectionRef{ .sink = idx };
                },
            }
            continue;
        }

        if (current == null) return ParseError.MissingSection;
        const kv = try splitKeyValue(trimmed);
        switch (current.?) {
            .source => |idx| try applySourceKV(arena, &source_drafts.items[idx], kv.key, kv.value),
            .transform => |idx| try applyTransformKV(arena, &transform_drafts.items[idx], kv.key, kv.value),
            .transform_limits => |idx| try applyTransformLimitsKV(arena, &transform_drafts.items[idx], kv.key, kv.value),
            .transform_sharding => |idx| try applyTransformShardingKV(arena, &transform_drafts.items[idx], kv.key, kv.value),
            .sink => |idx| try applySinkKV(arena, &sink_drafts.items[idx], kv.key, kv.value),
        }
    }

    var sources_out = std.ArrayListUnmanaged(cfg.SourceNode){};
    var transforms_out = std.ArrayListUnmanaged(cfg.TransformNode){};
    var sinks_out = std.ArrayListUnmanaged(cfg.SinkNode){};

    for (source_drafts.items) |*draft| {
        const entries_slice = try draft.entries.toOwnedSlice(arena);
        const table = source_cfg.TomlTable{ .entries = entries_slice };
        const source_config = try source_cfg.parseSourceConfig(arena, draft.id, &table);
        const outputs = draft.outputs orelse &[_][]const u8{};
        try sources_out.append(arena, .{ .id = draft.id, .config = source_config, .outputs = outputs });
    }

    for (transform_drafts.items) |draft| {
        const kind = draft.kind orelse return ParseError.MissingType;
        const inputs = draft.inputs orelse return ParseError.MissingInputs;
        const outputs = draft.outputs orelse &[_][]const u8{};
        switch (kind) {
            .sql => {
                const query = draft.query orelse return ParseError.MissingQuery;
                const default_queue = cfg.QueueConfig{};
                const default_limits = cfg.SqlLimitConfig{};
                if (draft.sharding_key_field != null and draft.sharding_key_metadata != null) {
                    return ParseError.InvalidValue;
                }
                const shard_count = draft.sharding_shard_count orelse 1;
                var sharding_key: ?cfg.SqlShardKey = null;
                if (draft.sharding_key_field) |field_name| {
                    sharding_key = cfg.SqlShardKey{ .field = field_name };
                } else if (draft.sharding_key_metadata) |metadata_key| {
                    sharding_key = cfg.SqlShardKey{ .metadata = metadata_key };
                }
                const sharding_config = cfg.SqlShardingConfig{
                    .shard_count = shard_count,
                    .key = sharding_key,
                    .fallback = draft.sharding_fallback orelse .route_first,
                };
                try transforms_out.append(arena, .{
                    .sql = .{
                        .id = draft.id,
                        .inputs = inputs,
                        .outputs = outputs,
                        .query = query,
                        .parallelism = draft.parallelism orelse 1,
                        .queue = cfg.QueueConfig{
                            .capacity = draft.queue_capacity orelse default_queue.capacity,
                            .strategy = draft.queue_strategy orelse default_queue.strategy,
                        },
                        .eviction = .{
                            .ttl_seconds = if (draft.eviction_ttl_seconds) |ttl| @intCast(ttl) else null,
                            .max_groups = draft.eviction_max_groups,
                            .sweep_interval_seconds = if (draft.eviction_sweep_seconds) |seconds| @intCast(seconds) else null,
                        },
                        .limits = .{
                            .max_state_bytes = draft.limits_max_state_bytes orelse default_limits.max_state_bytes,
                            .max_row_bytes = draft.limits_max_row_bytes orelse default_limits.max_row_bytes,
                            .max_group_bytes = draft.limits_max_group_bytes orelse default_limits.max_group_bytes,
                            .cpu_budget_ns_per_second = draft.limits_cpu_budget_ns_per_sec orelse default_limits.cpu_budget_ns_per_second,
                            .late_event_threshold_seconds = if (draft.limits_late_event_threshold_seconds) |secs| @intCast(secs) else default_limits.late_event_threshold_seconds,
                        },
                        .error_policy = draft.error_policy orelse .skip_event,
                        .sharding = sharding_config,
                        .event_time_field = draft.event_time_field,
                        .event_time_metadata = draft.event_time_metadata,
                        .watermark_lag_seconds = if (draft.watermark_lag_seconds) |secs| @intCast(secs) else null,
                        .allowed_lateness_seconds = if (draft.allowed_lateness_seconds) |secs| @intCast(secs) else null,
                    },
                });
            },
        }
    }

    for (sink_drafts.items) |draft| {
        const kind = draft.kind orelse return ParseError.MissingType;
        const inputs = draft.inputs orelse return ParseError.MissingInputs;
        switch (kind) {
            .console => {
                const default_queue = cfg.QueueConfig{};
                try sinks_out.append(arena, .{
                    .console = .{
                        .id = draft.id,
                        .inputs = inputs,
                        .target = draft.target,
                        .parallelism = draft.parallelism orelse 1,
                        .queue = cfg.QueueConfig{
                            .capacity = draft.queue_capacity orelse default_queue.capacity,
                            .strategy = draft.queue_strategy orelse default_queue.strategy,
                        },
                    },
                });
            },
        }
    }

    const pipeline = cfg.PipelineConfig{
        .sources = try sources_out.toOwnedSlice(arena),
        .transforms = try transforms_out.toOwnedSlice(arena),
        .sinks = try sinks_out.toOwnedSlice(arena),
    };

    return cfg.OwnedPipelineConfig{
        .arena = arena_ptr,
        .pipeline = pipeline,
    };
}

fn registerId(_: std.mem.Allocator, registry: *std.StringHashMap(void), id: []const u8) !void {
    const entry = try registry.getOrPut(id);
    if (entry.found_existing) return ParseError.DuplicateComponent;
    entry.value_ptr.* = {};
}

fn applySourceKV(arena: std.mem.Allocator, draft: *SourceDraft, key_raw: []const u8, value_raw: []const u8) !void {
    if (std.mem.eql(u8, key_raw, "outputs")) {
        if (draft.outputs != null) return ParseError.DuplicateKey;
        draft.outputs = try parseStringArray(arena, value_raw);
        return;
    }

    const value = try parseTomlValue(arena, value_raw);
    const key = try arena.dupe(u8, key_raw);
    try draft.entries.append(arena, .{ .key = key, .value = value });
}

fn applyTransformKV(arena: std.mem.Allocator, draft: *TransformDraft, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "type")) {
        if (draft.kind != null) return ParseError.DuplicateKey;
        const type_name = try parseStringValue(arena, value);
        if (std.mem.eql(u8, type_name, "sql")) {
            draft.kind = cfg.TransformType.sql;
        } else {
            return ParseError.UnsupportedTransformType;
        }
        return;
    }

    if (std.mem.eql(u8, key, "inputs")) {
        if (draft.inputs != null) return ParseError.DuplicateKey;
        draft.inputs = try parseStringArray(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "outputs")) {
        if (draft.outputs != null) return ParseError.DuplicateKey;
        draft.outputs = try parseStringArray(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "query")) {
        if (draft.query != null) return ParseError.DuplicateKey;
        draft.query = try parseStringValue(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "parallelism")) {
        if (draft.parallelism != null) return ParseError.DuplicateKey;
        draft.parallelism = try parsePositiveInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "queue_capacity")) {
        if (draft.queue_capacity != null) return ParseError.DuplicateKey;
        draft.queue_capacity = try parsePositiveInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "queue_strategy")) {
        if (draft.queue_strategy != null) return ParseError.DuplicateKey;
        const strategy_name = try parseStringValue(arena, value);
        draft.queue_strategy = try parseQueueStrategy(strategy_name);
        return;
    }

    if (std.mem.eql(u8, key, "eviction_ttl_seconds")) {
        if (draft.eviction_ttl_seconds != null) return ParseError.DuplicateKey;
        draft.eviction_ttl_seconds = try parseNonNegativeInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "eviction_max_groups")) {
        if (draft.eviction_max_groups != null) return ParseError.DuplicateKey;
        draft.eviction_max_groups = try parseNonNegativeInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "eviction_sweep_seconds")) {
        if (draft.eviction_sweep_seconds != null) return ParseError.DuplicateKey;
        draft.eviction_sweep_seconds = try parseNonNegativeInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "event_time_field")) {
        if (draft.event_time_field != null) return ParseError.DuplicateKey;
        draft.event_time_field = try parseStringValue(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "event_time_metadata")) {
        if (draft.event_time_metadata != null) return ParseError.DuplicateKey;
        draft.event_time_metadata = try parseEventTimeMetadata(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "watermark_lag_seconds")) {
        if (draft.watermark_lag_seconds != null) return ParseError.DuplicateKey;
        draft.watermark_lag_seconds = try parseNonNegativeInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "allowed_lateness_seconds")) {
        if (draft.allowed_lateness_seconds != null) return ParseError.DuplicateKey;
        draft.allowed_lateness_seconds = try parseNonNegativeInt(value);
        return;
    }

    if (std.mem.startsWith(u8, key, "limits_") and key.len > "limits_".len) {
        const bare_key = key["limits_".len..];
        return applyTransformLimitsKV(arena, draft, bare_key, value);
    }

    if (std.mem.eql(u8, key, "error_policy")) {
        if (draft.error_policy != null) return ParseError.DuplicateKey;
        draft.error_policy = try parseErrorPolicy(arena, value);
        return;
    }

    return ParseError.InvalidValue;
}

fn applyTransformLimitsKV(arena: std.mem.Allocator, draft: *TransformDraft, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "max_state_bytes")) {
        if (draft.limits_max_state_bytes != null) return ParseError.DuplicateKey;
        draft.limits_max_state_bytes = try parseByteQuantity(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "max_row_bytes")) {
        if (draft.limits_max_row_bytes != null) return ParseError.DuplicateKey;
        draft.limits_max_row_bytes = try parseByteQuantity(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "max_group_bytes")) {
        if (draft.limits_max_group_bytes != null) return ParseError.DuplicateKey;
        draft.limits_max_group_bytes = try parseByteQuantity(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "cpu_budget_ns_per_sec")) {
        if (draft.limits_cpu_budget_ns_per_sec != null) return ParseError.DuplicateKey;
        draft.limits_cpu_budget_ns_per_sec = try parseDurationNs(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "late_event_threshold_seconds")) {
        if (draft.limits_late_event_threshold_seconds != null) return ParseError.DuplicateKey;
        draft.limits_late_event_threshold_seconds = try parseNonNegativeInt(value);
        return;
    }

    return ParseError.InvalidValue;
}

fn applyTransformShardingKV(arena: std.mem.Allocator, draft: *TransformDraft, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "shard_count")) {
        if (draft.sharding_shard_count != null) return ParseError.DuplicateKey;
        draft.sharding_shard_count = try parsePositiveInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "key_field")) {
        if (draft.sharding_key_field != null) return ParseError.DuplicateKey;
        draft.sharding_key_field = try parseStringValue(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "key_metadata")) {
        if (draft.sharding_key_metadata != null) return ParseError.DuplicateKey;
        draft.sharding_key_metadata = try parseShardMetadataKey(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "fallback")) {
        if (draft.sharding_fallback != null) return ParseError.DuplicateKey;
        draft.sharding_fallback = try parseShardFallback(arena, value);
        return;
    }

    return ParseError.InvalidValue;
}

fn applySinkKV(arena: std.mem.Allocator, draft: *SinkDraft, key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "type")) {
        if (draft.kind != null) return ParseError.DuplicateKey;
        const type_name = try parseStringValue(arena, value);
        if (std.mem.eql(u8, type_name, "console")) {
            draft.kind = cfg.SinkType.console;
        } else {
            return ParseError.UnsupportedSinkType;
        }
        return;
    }

    if (std.mem.eql(u8, key, "inputs")) {
        if (draft.inputs != null) return ParseError.DuplicateKey;
        draft.inputs = try parseStringArray(arena, value);
        return;
    }

    if (std.mem.eql(u8, key, "target")) {
        const target_name = try parseStringValue(arena, value);
        if (std.mem.eql(u8, target_name, "stdout")) {
            draft.target = .stdout;
        } else if (std.mem.eql(u8, target_name, "stderr")) {
            draft.target = .stderr;
        } else {
            return ParseError.InvalidValue;
        }
        return;
    }

    if (std.mem.eql(u8, key, "parallelism")) {
        if (draft.parallelism != null) return ParseError.DuplicateKey;
        draft.parallelism = try parsePositiveInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "queue_capacity")) {
        if (draft.queue_capacity != null) return ParseError.DuplicateKey;
        draft.queue_capacity = try parsePositiveInt(value);
        return;
    }

    if (std.mem.eql(u8, key, "queue_strategy")) {
        if (draft.queue_strategy != null) return ParseError.DuplicateKey;
        const strategy_name = try parseStringValue(arena, value);
        draft.queue_strategy = try parseQueueStrategy(strategy_name);
        return;
    }

    return ParseError.InvalidValue;
}

fn parseTomlValue(arena: std.mem.Allocator, raw: []const u8) !source_cfg.TomlValue {
    if (raw.len == 0) return ParseError.InvalidValue;
    if (raw[0] == '[') {
        const array = try parseStringArray(arena, raw);
        return source_cfg.TomlValue{ .string_array = array };
    }
    if (raw[0] == '"') {
        const value = try parseStringValue(arena, raw);
        return source_cfg.TomlValue{ .string = value };
    }

    if (std.mem.eql(u8, raw, "true")) return source_cfg.TomlValue{ .bool = true };
    if (std.mem.eql(u8, raw, "false")) return source_cfg.TomlValue{ .bool = false };

    if (raw[0] == '-' or ascii.isDigit(raw[0])) {
        const number = std.fmt.parseInt(i64, raw, 10) catch return ParseError.InvalidValue;
        return source_cfg.TomlValue{ .integer = number };
    }

    return ParseError.InvalidValue;
}

fn parseStringArray(arena: std.mem.Allocator, raw: []const u8) ![]const []const u8 {
    var i: usize = 0;
    if (raw.len < 2 or raw[0] != '[') return ParseError.InvalidSyntax;
    if (raw[raw.len - 1] != ']') return ParseError.InvalidSyntax;

    i = 1;
    var list = std.ArrayListUnmanaged([]const u8){};

    while (true) {
        i = skipWhitespace(raw, i);
        if (i >= raw.len) return ParseError.InvalidSyntax;
        if (raw[i] == ']') {
            i += 1;
            break;
        }
        const string_value = try parseStringLiteral(arena, raw, &i);
        try list.append(arena, string_value);
        i = skipWhitespace(raw, i);
        if (i >= raw.len) return ParseError.InvalidSyntax;
        if (raw[i] == ',') {
            i += 1;
            continue;
        }
        if (raw[i] == ']') {
            i += 1;
            break;
        }
        return ParseError.InvalidSyntax;
    }

    return try list.toOwnedSlice(arena);
}

fn parseLiteralSlice(arena: std.mem.Allocator, raw: []const u8) ![]const u8 {
    if (raw.len != 0 and raw[0] == '"') {
        return parseStringValue(arena, raw);
    }
    return std.mem.trim(u8, raw, " \t\r");
}

fn parseByteQuantity(arena: std.mem.Allocator, raw: []const u8) !usize {
    const literal = try parseLiteralSlice(arena, raw);
    if (literal.len == 0) return ParseError.InvalidValue;

    var idx: usize = 0;
    while (idx < literal.len and ascii.isDigit(literal[idx])) : (idx += 1) {}
    if (idx == 0) return ParseError.InvalidValue;

    const number_slice = literal[0..idx];
    const base_value = std.fmt.parseInt(u64, number_slice, 10) catch return ParseError.InvalidValue;
    const suffix = std.mem.trim(u8, literal[idx..], " \t\r");

    var multiplier: u64 = 1;
    if (suffix.len != 0) {
        if (ascii.eqlIgnoreCase(suffix, "b")) {
            multiplier = 1;
        } else if (ascii.eqlIgnoreCase(suffix, "kib")) {
            multiplier = 1024;
        } else if (ascii.eqlIgnoreCase(suffix, "mib")) {
            multiplier = 1024 * 1024;
        } else if (ascii.eqlIgnoreCase(suffix, "gib")) {
            multiplier = 1024 * 1024 * 1024;
        } else if (ascii.eqlIgnoreCase(suffix, "kb")) {
            multiplier = 1000;
        } else if (ascii.eqlIgnoreCase(suffix, "mb")) {
            multiplier = 1000 * 1000;
        } else if (ascii.eqlIgnoreCase(suffix, "gb")) {
            multiplier = 1000 * 1000 * 1000;
        } else {
            return ParseError.InvalidValue;
        }
    }

    const total = @as(u128, base_value) * multiplier;
    if (total == 0) return ParseError.InvalidValue;
    if (total > std.math.maxInt(usize)) return ParseError.InvalidValue;
    return @intCast(total);
}

fn parseDurationNs(arena: std.mem.Allocator, raw: []const u8) !u64 {
    const literal = try parseLiteralSlice(arena, raw);
    if (literal.len == 0) return ParseError.InvalidValue;

    var idx: usize = 0;
    while (idx < literal.len and ascii.isDigit(literal[idx])) : (idx += 1) {}
    if (idx == 0) return ParseError.InvalidValue;

    const number_slice = literal[0..idx];
    const magnitude = std.fmt.parseInt(u64, number_slice, 10) catch return ParseError.InvalidValue;
    const suffix = std.mem.trim(u8, literal[idx..], " \t\r");

    var multiplier: u64 = 1;
    if (suffix.len == 0 or ascii.eqlIgnoreCase(suffix, "ns")) {
        multiplier = 1;
    } else if (ascii.eqlIgnoreCase(suffix, "us") or std.mem.eql(u8, suffix, "\xC2\xB5s")) {
        multiplier = std.time.ns_per_us;
    } else if (ascii.eqlIgnoreCase(suffix, "ms")) {
        multiplier = std.time.ns_per_ms;
    } else if (ascii.eqlIgnoreCase(suffix, "s")) {
        multiplier = std.time.ns_per_s;
    } else {
        return ParseError.InvalidValue;
    }

    const total = @as(u128, magnitude) * multiplier;
    if (total == 0) return ParseError.InvalidValue;
    if (total > std.math.maxInt(u64)) return ParseError.InvalidValue;
    return @intCast(total);
}

fn parseErrorPolicy(arena: std.mem.Allocator, raw: []const u8) !cfg.SqlErrorPolicy {
    const literal = try parseLiteralSlice(arena, raw);
    if (ascii.eqlIgnoreCase(literal, "skip_event")) return .skip_event;
    if (ascii.eqlIgnoreCase(literal, "null")) return .null;
    if (ascii.eqlIgnoreCase(literal, "clamp")) return .clamp;
    if (ascii.eqlIgnoreCase(literal, "error")) return .propagate;
    return ParseError.InvalidValue;
}

fn parseShardFallback(arena: std.mem.Allocator, raw: []const u8) !cfg.SqlShardFallback {
    const literal = try parseLiteralSlice(arena, raw);
    if (ascii.eqlIgnoreCase(literal, "first_shard")) return .route_first;
    if (ascii.eqlIgnoreCase(literal, "route_first")) return .route_first;
    if (ascii.eqlIgnoreCase(literal, "drop")) return .drop;
    return ParseError.InvalidValue;
}

fn parseShardMetadataKey(arena: std.mem.Allocator, raw: []const u8) !cfg.SqlShardMetadataKey {
    const literal = try parseLiteralSlice(arena, raw);
    if (ascii.eqlIgnoreCase(literal, "source_id")) return .source_id;
    return ParseError.InvalidValue;
}

fn parseEventTimeMetadata(arena: std.mem.Allocator, raw: []const u8) !cfg.SqlEventTimeMetadata {
    const literal = try parseLiteralSlice(arena, raw);
    if (ascii.eqlIgnoreCase(literal, "received_at")) return .received_at;
    return ParseError.InvalidValue;
}

fn parsePositiveInt(raw: []const u8) !usize {
    if (raw.len == 0) return ParseError.InvalidValue;
    const value = std.fmt.parseInt(usize, raw, 10) catch return ParseError.InvalidValue;
    if (value == 0) return ParseError.InvalidValue;
    return value;
}

fn parseNonNegativeInt(raw: []const u8) !usize {
    if (raw.len == 0) return ParseError.InvalidValue;
    return std.fmt.parseInt(usize, raw, 10) catch ParseError.InvalidValue;
}

fn parseQueueStrategy(name: []const u8) !cfg.QueueStrategy {
    if (std.mem.eql(u8, name, "reject")) return .reject;
    if (std.mem.eql(u8, name, "drop_newest")) return .drop_newest;
    if (std.mem.eql(u8, name, "drop_oldest")) return .drop_oldest;
    return ParseError.InvalidValue;
}

fn parseStringValue(arena: std.mem.Allocator, raw: []const u8) ![]const u8 {
    var index: usize = 0;
    return try parseStringLiteral(arena, raw, &index);
}

fn parseStringLiteral(arena: std.mem.Allocator, raw: []const u8, index_ptr: *usize) ![]const u8 {
    var index = index_ptr.*;
    if (index >= raw.len or raw[index] != '"') return ParseError.InvalidSyntax;
    index += 1;
    var buffer = std.ArrayListUnmanaged(u8){};

    while (index < raw.len) : (index += 1) {
        const ch = raw[index];
        if (ch == '"') {
            index += 1;
            index_ptr.* = index;
            return try buffer.toOwnedSlice(arena);
        }
        if (ch == '\\') {
            index += 1;
            if (index >= raw.len) return ParseError.InvalidSyntax;
            const escaped: u8 = switch (raw[index]) {
                '"' => @as(u8, '"'),
                '\\' => @as(u8, '\\'),
                'n' => @as(u8, '\n'),
                'r' => @as(u8, '\r'),
                't' => @as(u8, '\t'),
                else => return ParseError.InvalidValue,
            };
            try buffer.append(arena, escaped);
            continue;
        }
        try buffer.append(arena, ch);
    }

    return ParseError.InvalidSyntax;
}

fn skipWhitespace(raw: []const u8, start: usize) usize {
    var i = start;
    while (i < raw.len and (raw[i] == ' ' or raw[i] == '\t' or raw[i] == '\r')) : (i += 1) {}
    return i;
}

const KeyValue = struct {
    key: []const u8,
    value: []const u8,
};

fn splitKeyValue(line: []const u8) !KeyValue {
    var in_string = false;
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const ch = line[i];
        if (ch == '"' and (i == 0 or line[i - 1] != '\\')) {
            in_string = !in_string;
        }
        if (!in_string and ch == '=') {
            const key = std.mem.trim(u8, line[0..i], " \t\r");
            const value = std.mem.trim(u8, line[i + 1 ..], " \t\r");
            if (key.len == 0 or value.len == 0) return ParseError.InvalidSyntax;
            return KeyValue{ .key = key, .value = value };
        }
    }
    return ParseError.InvalidSyntax;
}

fn parseSection(arena: std.mem.Allocator, line: []const u8) !struct { kind: SectionKind, id: []const u8, subsection: ?[]const u8 } {
    if (line.len < 3 or line[line.len - 1] != ']') return ParseError.InvalidSyntax;
    const inner = std.mem.trim(u8, line[1 .. line.len - 1], " \t\r");
    const dot = std.mem.indexOfScalar(u8, inner, '.');
    if (dot == null) return ParseError.InvalidSyntax;
    const prefix = std.mem.trim(u8, inner[0..dot.?], " \t\r");
    const rest = std.mem.trim(u8, inner[dot.? + 1 ..], " \t\r");
    if (rest.len == 0) return ParseError.InvalidSyntax;
    const kind = if (std.mem.eql(u8, prefix, "sources")) SectionKind.sources else if (std.mem.eql(u8, prefix, "transforms")) SectionKind.transforms else if (std.mem.eql(u8, prefix, "sinks")) SectionKind.sinks else return ParseError.UnknownSection;
    const sub_dot = std.mem.indexOfScalar(u8, rest, '.');

    var id_part = rest;
    var subsection_part: ?[]const u8 = null;

    if (sub_dot) |idx| {
        if (kind != SectionKind.transforms) return ParseError.InvalidSyntax;
        id_part = std.mem.trim(u8, rest[0..idx], " \t\r");
        const raw_subsection = std.mem.trim(u8, rest[idx + 1 ..], " \t\r");
        if (raw_subsection.len == 0) return ParseError.InvalidSyntax;
        if (std.mem.indexOfScalar(u8, raw_subsection, '.')) |_| {
            return ParseError.InvalidSyntax;
        }
        subsection_part = try arena.dupe(u8, raw_subsection);
    }

    if (id_part.len == 0) return ParseError.InvalidSyntax;
    const id = try arena.dupe(u8, id_part);
    return .{ .kind = kind, .id = id, .subsection = subsection_part };
}

fn stripComment(line: []const u8) []const u8 {
    var in_string = false;
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const ch = line[i];
        if (ch == '"' and (i == 0 or line[i - 1] != '\\')) {
            in_string = !in_string;
        }
        if (!in_string and ch == '#') {
            return line[0..i];
        }
    }
    return line;
}

const testing = std.testing;

fn sampleConfig() []const u8 {
    return ("[sources.syslog_in]\n" ++
        "type = \"syslog\"\n" ++
        "address = \"udp://0.0.0.0:514\"\n" ++
        "\n" ++
        "[transforms.sql_filter]\n" ++
        "type = \"sql\"\n" ++
        "inputs = [\"syslog_in\"]\n" ++
        "outputs = [\"console\"]\n" ++
        "query = \"SELECT * FROM logs\"\n" ++
        "\n" ++
        "[sinks.console]\n" ++
        "type = \"console\"\n" ++
        "inputs = [\"sql_filter\"]\n" ++
        "target = \"stdout\"\n");
}

test "parse sample config" {
    var owned = try parse(testing.allocator, sampleConfig());
    defer owned.deinit(testing.allocator);
    const pipeline = owned.pipeline;
    try testing.expect(pipeline.sources.len == 1);
    try testing.expect(pipeline.transforms.len == 1);
    try testing.expect(pipeline.sinks.len == 1);
    const sql_transform = pipeline.transforms[0].sql;
    try testing.expect(sql_transform.limits.late_event_threshold_seconds == null);
    try testing.expectEqual(@as(usize, 1), sql_transform.sharding.shard_count);
    try testing.expect(sql_transform.sharding.key == null);
    try pipeline.validate(testing.allocator);
}

test "parse execution settings" {
    const config = ("[sources.syslog_in]\n" ++
        "type = \"syslog\"\n" ++
        "address = \"udp://0.0.0.0:514\"\n" ++
        "\n" ++
        "[transforms.sql_filter]\n" ++
        "type = \"sql\"\n" ++
        "inputs = [\"syslog_in\"]\n" ++
        "outputs = [\"console\"]\n" ++
        "query = \"SELECT * FROM logs\"\n" ++
        "parallelism = 4\n" ++
        "queue_capacity = 2048\n" ++
        "queue_strategy = \"drop_oldest\"\n" ++
        "\n" ++
        "[sinks.console]\n" ++
        "type = \"console\"\n" ++
        "inputs = [\"sql_filter\"]\n" ++
        "target = \"stdout\"\n" ++
        "parallelism = 2\n" ++
        "queue_capacity = 512\n" ++
        "queue_strategy = \"drop_newest\"\n");

    var owned = try parse(testing.allocator, config);
    defer owned.deinit(testing.allocator);
    const pipeline = owned.pipeline;
    try testing.expectEqual(@as(usize, 1), pipeline.sources.len);
    try testing.expectEqual(@as(usize, 1), pipeline.transforms.len);
    try testing.expectEqual(@as(usize, 1), pipeline.sinks.len);

    const transform_settings = pipeline.transforms[0].executionSettings();
    try testing.expectEqual(@as(usize, 4), transform_settings.parallelism);
    try testing.expectEqual(@as(usize, 2048), transform_settings.queue.capacity);
    try testing.expect(transform_settings.queue.strategy == .drop_oldest);

    const sink_settings = pipeline.sinks[0].executionSettings();
    try testing.expectEqual(@as(usize, 2), sink_settings.parallelism);
    try testing.expectEqual(@as(usize, 512), sink_settings.queue.capacity);
    try testing.expect(sink_settings.queue.strategy == .drop_newest);
}

test "parse sharding config" {
    const config = ("[sources.syslog_in]\n" ++
        "type = \"syslog\"\n" ++
        "address = \"udp://127.0.0.1:5514\"\n" ++
        "\n" ++
        "[transforms.sql_sharded]\n" ++
        "type = \"sql\"\n" ++
        "inputs = [\"syslog_in\"]\n" ++
        "query = \"SELECT message FROM logs\"\n" ++
        "\n" ++
        "[transforms.sql_sharded.sharding]\n" ++
        "shard_count = 4\n" ++
        "key_field = \"hostname\"\n" ++
        "fallback = \"drop\"\n" ++
        "\n" ++
        "[sinks.console]\n" ++
        "type = \"console\"\n" ++
        "inputs = [\"sql_sharded\"]\n");

    var owned = try parse(testing.allocator, config);
    defer owned.deinit(testing.allocator);

    const pipeline = owned.pipeline;
    try testing.expectEqual(@as(usize, 1), pipeline.transforms.len);
    const sql_transform = pipeline.transforms[0].sql;
    try testing.expectEqual(@as(usize, 4), sql_transform.sharding.shard_count);
    const shard_key = sql_transform.sharding.key orelse return testing.expect(false);
    try testing.expect(shard_key == .field);
    try testing.expect(std.mem.eql(u8, shard_key.field, "hostname"));
    try testing.expect(sql_transform.sharding.fallback == .drop);
    try pipeline.validate(testing.allocator);
}

test "parse sharding metadata key" {
    const config = ("[sources.syslog_in]\n" ++
        "type = \"syslog\"\n" ++
        "address = \"udp://127.0.0.1:5514\"\n" ++
        "\n" ++
        "[transforms.sql_meta]\n" ++
        "type = \"sql\"\n" ++
        "inputs = [\"syslog_in\"]\n" ++
        "query = \"SELECT * FROM logs\"\n" ++
        "\n" ++
        "[transforms.sql_meta.sharding]\n" ++
        "shard_count = 2\n" ++
        "key_metadata = \"source_id\"\n" ++
        "\n" ++
        "[sinks.console]\n" ++
        "type = \"console\"\n" ++
        "inputs = [\"sql_meta\"]\n");

    var owned = try parse(testing.allocator, config);
    defer owned.deinit(testing.allocator);

    const sql_transform = owned.pipeline.transforms[0].sql;
    try testing.expectEqual(@as(usize, 2), sql_transform.sharding.shard_count);
    const shard_key = sql_transform.sharding.key orelse return testing.expect(false);
    try testing.expect(shard_key == .metadata);
    try testing.expect(shard_key.metadata == .source_id);
}

test "parse nested transform limits table" {
    const config = ("[sources.syslog_in]\n" ++
        "type = \"syslog\"\n" ++
        "address = \"udp://127.0.0.1:5514\"\n" ++
        "\n" ++
        "[transforms.sql_enrich]\n" ++
        "type = \"sql\"\n" ++
        "inputs = [\"syslog_in\"]\n" ++
        "query = \"SELECT message, COUNT(*) AS total FROM logs GROUP BY message\"\n" ++
        "error_policy = \"clamp\"\n" ++
        "\n" ++
        "[transforms.sql_enrich.limits]\n" ++
        "max_state_bytes = \"32MiB\"\n" ++
        "max_row_bytes = \"16KiB\"\n" ++
        "max_group_bytes = \"512KiB\"\n" ++
        "cpu_budget_ns_per_sec = \"50ms\"\n" ++
        "late_event_threshold_seconds = 5\n" ++
        "\n" ++
        "[sinks.console]\n" ++
        "type = \"console\"\n" ++
        "inputs = [\"sql_enrich\"]\n" ++
        "target = \"stdout\"\n");

    var owned = try parse(testing.allocator, config);
    defer owned.deinit(testing.allocator);
    const pipeline = owned.pipeline;
    try testing.expectEqual(@as(usize, 1), pipeline.sources.len);
    try testing.expectEqual(@as(usize, 1), pipeline.transforms.len);
    try testing.expectEqual(@as(usize, 1), pipeline.sinks.len);

    const sql_transform = pipeline.transforms[0].sql;
    try testing.expectEqual(@as(usize, 32 * 1024 * 1024), sql_transform.limits.max_state_bytes);
    try testing.expectEqual(@as(usize, 16 * 1024), sql_transform.limits.max_row_bytes);
    try testing.expectEqual(@as(usize, 512 * 1024), sql_transform.limits.max_group_bytes);
    try testing.expectEqual(@as(u64, 50 * std.time.ns_per_ms), sql_transform.limits.cpu_budget_ns_per_second);
    try testing.expect(sql_transform.limits.late_event_threshold_seconds != null);
    try testing.expectEqual(@as(u64, 5), sql_transform.limits.late_event_threshold_seconds.?);
    try testing.expect(sql_transform.error_policy == .clamp);
}

test "parseFile enforces size limit" {
    const allocator = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const filename = "oversized.toml";
    var file = try tmp.dir.createFile(filename, .{});
    defer file.close();

    const chunk = [_]u8{0} ** 4096;
    var remaining: usize = max_config_bytes + 1;
    while (remaining > 0) {
        const to_write = if (remaining > chunk.len) chunk.len else remaining;
        try file.writeAll(chunk[0..to_write]);
        remaining -= to_write;
    }
    try file.sync();

    const absolute_path = try tmp.dir.realpathAlloc(allocator, filename);
    defer allocator.free(absolute_path);

    try testing.expectError(ParseError.ConfigTooLarge, parseFile(allocator, absolute_path));
}
