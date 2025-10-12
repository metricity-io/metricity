const std = @import("std");
const posix = std.posix;
const src = @import("source.zig");
const cfg = @import("config.zig");
const event = @import("event.zig");
const buffer = @import("buffer.zig");
const frame = @import("syslog/frame.zig");
const arena_pool = @import("arena_pool.zig");

const builtin = @import("builtin");
const unicode = std.unicode;
const json = std.json;
const ArrayListManaged = std.array_list.Managed;

const has_posix_regex = switch (builtin.target.os.tag) {
    .linux, .macos, .ios, .freebsd, .netbsd, .dragonfly, .openbsd, .aix, .solaris => true,
    else => false,
};

const c = if (has_posix_regex) @cImport({
    @cInclude("regex_helper.h");
}) else struct {};

const RegexError = if (has_posix_regex) error{ InvalidPattern, RegexExecFailed } else error{};
const PatternError = RegexError || std.mem.Allocator.Error;

const RegexMatcher = if (has_posix_regex) struct {
    regex: c.metricity_regex_t,

    fn init(allocator: std.mem.Allocator, pattern: []const u8) PatternError!RegexMatcher {
        var matcher = RegexMatcher{
            .regex = undefined,
        };
        c.metricity_regex_init(&matcher.regex);

        const pattern_z = try allocator.dupeZ(u8, pattern);
        defer allocator.free(pattern_z);

        const res = c.regcomp(c.metricity_regex_ptr(&matcher.regex), pattern_z.ptr, c.REG_EXTENDED | c.REG_NEWLINE);
        if (res != 0) return RegexError.InvalidPattern;
        return matcher;
    }

    fn deinit(self: *RegexMatcher) void {
        c.regfree(c.metricity_regex_ptr(&self.regex));
    }

    fn matches(self: *const RegexMatcher, allocator: std.mem.Allocator, line: []const u8) PatternError!bool {
        const line_z = try allocator.dupeZ(u8, line);
        defer allocator.free(line_z);

        const res = c.regexec(c.metricity_regex_ptr_const(&self.regex), line_z.ptr, 0, null, 0);
        return switch (res) {
            0 => true,
            c.REG_NOMATCH => false,
            else => RegexError.RegexExecFailed,
        };
    }
} else struct {
    fn init(_: std.mem.Allocator, _: []const u8) PatternError!RegexMatcher {
        return .{};
    }

    fn deinit(_: *RegexMatcher) void {}

    fn matches(_: *const RegexMatcher, _: std.mem.Allocator, _: []const u8) PatternError!bool {
        return true;
    }
};

pub fn factory() src.SourceFactory {
    return .{ .type = .stdin, .create = create };
}

const TimeSource = *const fn () i128;

fn defaultTimeSource() i128 {
    return std.time.nanoTimestamp();
}

const RateLimiter = struct {
    tokens: f64,
    capacity: f64,
    rate_per_sec: f64,
    last_ns: i128,

    fn init(now_ns: i128, rate_per_sec: usize, burst: usize) RateLimiter {
        const capacity = @as(f64, @floatFromInt(burst));
        return .{
            .tokens = capacity,
            .capacity = capacity,
            .rate_per_sec = @as(f64, @floatFromInt(rate_per_sec)),
            .last_ns = now_ns,
        };
    }

    fn allow(self: *RateLimiter, now_ns: i128) bool {
        var elapsed: i128 = now_ns - self.last_ns;
        if (elapsed < 0) elapsed = 0;
        const elapsed_float = @as(f64, @floatFromInt(@as(u128, @intCast(elapsed))));
        const ns_per_second = @as(f64, @floatFromInt(std.time.ns_per_s));
        const replenished = self.rate_per_sec * (elapsed_float / ns_per_second);
        self.tokens = @min(self.capacity, self.tokens + replenished);
        self.last_ns = now_ns;
        if (self.tokens >= 1.0) {
            self.tokens -= 1.0;
            return true;
        }
        return false;
    }
};

const EventBuffer = buffer.BoundedBuffer(event.ManagedEvent);

const DecodeError = error{
    InvalidJson,
    InvalidUtf8,
};

const DecodeOutcome = struct {
    managed: event.ManagedEvent,
    truncated: bool,
};

const Aggregated = struct {
    bytes: []u8,
    truncated: bool,
};

const EventAllocation = struct {
    pool: *arena_pool.EventArenaPool,
    arena: *arena_pool.EventArenaPool.Arena,

    fn release(context: ?*anyopaque) void {
        const raw = context orelse return;
        const aligned: *align(@alignOf(EventAllocation)) anyopaque = @alignCast(raw);
        const allocation: *EventAllocation = @ptrCast(aligned);

        allocation.pool.release(allocation.arena);
        allocation.pool.allocator.destroy(allocation);
    }
};

const Decoder = struct {
    allocator: std.mem.Allocator,
    codec: cfg.StdinCodec,
    allow_invalid_utf8: bool,
    json_message_key: []const u8,
    on_decode_error: cfg.StdinDecodeErrorAction,

    fn init(
        allocator: std.mem.Allocator,
        config: cfg.StdinConfig,
    ) Decoder {
        return .{
            .allocator = allocator,
            .codec = config.codec,
            .allow_invalid_utf8 = config.allow_invalid_utf8,
            .json_message_key = config.json_message_key,
            .on_decode_error = config.on_decode_error,
        };
    }

    fn decode(
        self: *Decoder,
        state: *StdinState,
        arena: *arena_pool.EventArenaPool.Arena,
        bytes: []u8,
        truncated: bool,
        now_ns: i128,
    ) !DecodeOutcome {
        return switch (self.codec) {
            .text => self.decodeText(state, arena, bytes, truncated, now_ns),
            .raw => self.decodeRaw(state, arena, bytes, truncated, now_ns),
            .ndjson => self.decodeNdjson(state, arena, bytes, truncated, now_ns),
        };
    }

    fn decodeText(
        self: *Decoder,
        state: *StdinState,
        arena: *arena_pool.EventArenaPool.Arena,
        bytes: []u8,
        truncated: bool,
        now_ns: i128,
    ) !DecodeOutcome {
        const arena_allocator = arena.allocator();
        const message_copy = try arena_allocator.dupe(u8, bytes);

        var fields = ArrayListManaged(event.Field).init(arena_allocator);
        errdefer fields.deinit();

        var effective_truncated = truncated;
        if (!self.allow_invalid_utf8) {
            const valid = unicode.utf8ValidateSlice(bytes);
            if (!valid) {
                effective_truncated = true;
                try appendBooleanField(&fields, arena_allocator, UTF8_VALID_FIELD_NAME, false);
            }
        }

        if (state.config.include_source_field) {
            try appendStringField(&fields, arena_allocator, SOURCE_FIELD_NAME, SOURCE_FIELD_VALUE);
        }

        const field_slice = try fields.toOwnedSlice();
        const payload = event.Payload{
            .log = .{
                .message = message_copy,
                .fields = field_slice,
            },
        };

        const managed = try createManagedEvent(state, arena, payload, effective_truncated, now_ns);
        return DecodeOutcome{ .managed = managed, .truncated = effective_truncated };
    }

    fn decodeRaw(
        self: *Decoder,
        state: *StdinState,
        arena: *arena_pool.EventArenaPool.Arena,
        bytes: []u8,
        truncated: bool,
        now_ns: i128,
    ) !DecodeOutcome {
        _ = self;
        const arena_allocator = arena.allocator();
        const message_copy = try arena_allocator.dupe(u8, bytes);

        var fields = ArrayListManaged(event.Field).init(arena_allocator);
        errdefer fields.deinit();

        if (state.config.include_source_field) {
            try appendStringField(&fields, arena_allocator, SOURCE_FIELD_NAME, SOURCE_FIELD_VALUE);
        }

        const field_slice = try fields.toOwnedSlice();
        const payload = event.Payload{
            .log = .{
                .message = message_copy,
                .fields = field_slice,
            },
        };

        const managed = try createManagedEvent(state, arena, payload, truncated, now_ns);
        return DecodeOutcome{ .managed = managed, .truncated = truncated };
    }

    fn decodeNdjson(
        self: *Decoder,
        state: *StdinState,
        arena: *arena_pool.EventArenaPool.Arena,
        bytes: []u8,
        truncated: bool,
        now_ns: i128,
    ) !DecodeOutcome {
        var parsed = std.json.parseFromSlice(std.json.Value, state.allocator, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => {
                return self.handleDecodeError(state, arena, bytes, truncated, now_ns);
            },
        };
        defer parsed.deinit();

        if (parsed.value != .object) {
            return self.handleDecodeError(state, arena, bytes, truncated, now_ns);
        }

        const arena_allocator = arena.allocator();
        var fields = ArrayListManaged(event.Field).init(arena_allocator);
        errdefer fields.deinit();

        var message_source: []const u8 = bytes;

        var it = parsed.value.object.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            const val = entry.value_ptr.*;

            if (self.json_message_key.len != 0 and std.mem.eql(u8, key, self.json_message_key)) {
                if (val == .string) {
                    message_source = val.string;
                }
            }

            const converted = try jsonValueToEventValue(state.allocator, arena_allocator, val);
            try appendValueField(&fields, arena_allocator, key, converted);
        }

        if (state.config.include_source_field) {
            try appendStringField(&fields, arena_allocator, SOURCE_FIELD_NAME, SOURCE_FIELD_VALUE);
        }

        const message_copy = try arena_allocator.dupe(u8, message_source);
        const field_slice = try fields.toOwnedSlice();
        const payload = event.Payload{
            .log = .{
                .message = message_copy,
                .fields = field_slice,
            },
        };

        const managed = try createManagedEvent(state, arena, payload, truncated, now_ns);
        return DecodeOutcome{ .managed = managed, .truncated = truncated };
    }

    fn handleDecodeError(
        self: *Decoder,
        state: *StdinState,
        arena: *arena_pool.EventArenaPool.Arena,
        bytes: []u8,
        truncated: bool,
        now_ns: i128,
    ) !DecodeOutcome {
        recordDecodeError(state, 1);
        return switch (self.on_decode_error) {
            .drop => DecodeError.InvalidJson,
            .as_text => self.decodeText(state, arena, bytes, truncated, now_ns),
        };
    }
};

const SOURCE_FIELD_NAME = "source";
const SOURCE_FIELD_VALUE = "stdin";
const UTF8_VALID_FIELD_NAME = "stdin_msg_utf8_valid";

fn appendStringField(
    fields: *ArrayListManaged(event.Field),
    arena_allocator: std.mem.Allocator,
    name: []const u8,
    value: []const u8,
) !void {
    const name_copy = try arena_allocator.dupe(u8, name);
    const value_copy = try arena_allocator.dupe(u8, value);
    try fields.append(.{ .name = name_copy, .value = .{ .string = value_copy } });
}

fn appendBooleanField(
    fields: *ArrayListManaged(event.Field),
    arena_allocator: std.mem.Allocator,
    name: []const u8,
    flag: bool,
) !void {
    const name_copy = try arena_allocator.dupe(u8, name);
    try fields.append(.{ .name = name_copy, .value = .{ .boolean = flag } });
}

fn appendValueField(
    fields: *ArrayListManaged(event.Field),
    arena_allocator: std.mem.Allocator,
    name: []const u8,
    value: event.Value,
) !void {
    const name_copy = try arena_allocator.dupe(u8, name);
    try fields.append(.{ .name = name_copy, .value = value });
}

fn createManagedEvent(
    state: *StdinState,
    arena: *arena_pool.EventArenaPool.Arena,
    payload: event.Payload,
    truncated: bool,
    now_ns: i128,
) !event.ManagedEvent {
    const allocation = try state.allocator.create(EventAllocation);
    allocation.* = .{
        .pool = &state.arena_pool,
        .arena = arena,
    };

    const metadata = event.EventMetadata{
        .received_at = if (state.config.set_received_at) now_ns else null,
        .source_id = state.descriptor.name,
        .transport = null,
        .payload_truncated = truncated,
    };

    const finalizer = event.EventFinalizer.init(EventAllocation.release, allocation);
    return event.ManagedEvent.init(event.Event{
        .metadata = metadata,
        .payload = payload,
    }, finalizer);
}

fn jsonValueToEventValue(
    gpa: std.mem.Allocator,
    arena_allocator: std.mem.Allocator,
    value: std.json.Value,
) !event.Value {
    switch (value) {
        .null => return .null,
        .bool => |b| return .{ .boolean = b },
        .integer => |i| return .{ .integer = i },
        .float => |f| return .{ .float = f },
        .number_string => |s| {
            const copy = try arena_allocator.dupe(u8, s);
            return .{ .string = copy };
        },
        .string => |s| {
            const copy = try arena_allocator.dupe(u8, s);
            return .{ .string = copy };
        },
        .array => {
            const serialized = std.json.Stringify.valueAlloc(gpa, value, .{}) catch return error.OutOfMemory;
            defer gpa.free(serialized);
            const copy = try arena_allocator.dupe(u8, serialized);
            return .{ .string = copy };
        },
        .object => {
            const serialized = std.json.Stringify.valueAlloc(gpa, value, .{}) catch return error.OutOfMemory;
            defer gpa.free(serialized);
            const copy = try arena_allocator.dupe(u8, serialized);
            return .{ .string = copy };
        },
    }
}

const MultilineState = struct {
    allocator: std.mem.Allocator,
    mode: cfg.MultilineMode,
    buffer: ArrayListManaged(u8),
    pending_truncated: bool = false,
    start_pattern: ?[]const u8 = null,
    timeout_ns: i128,
    max_bytes: usize,
    has_pending: bool = false,
    last_append_ns: i128 = 0,
    compiled_regex: ?RegexMatcher = null,

    fn init(allocator: std.mem.Allocator, config: cfg.StdinConfig) PatternError!MultilineState {
        var state = MultilineState{
            .allocator = allocator,
            .mode = config.multiline_mode,
            .buffer = ArrayListManaged(u8).init(allocator),
            .timeout_ns = @as(i128, config.multiline_timeout_ms) * std.time.ns_per_ms,
            .max_bytes = config.multiline_max_bytes,
            .start_pattern = config.multiline_start_pattern,
        };

        if (comptime has_posix_regex) {
            if (config.multiline_mode == .pattern) {
                if (config.multiline_start_pattern) |pattern| {
                    state.compiled_regex = try RegexMatcher.init(allocator, pattern);
                }
            }
        }

        return state;
    }

    fn deinit(self: *MultilineState) void {
        self.buffer.deinit();
        if (comptime has_posix_regex) {
            if (self.compiled_regex) |*compiled| {
                compiled.deinit();
            }
        }
    }

    fn push(
        self: *MultilineState,
        allocator: std.mem.Allocator,
        line: []u8,
        truncated: bool,
        now_ns: i128,
    ) PatternError!?Aggregated {
        switch (self.mode) {
            .disabled => {
                self.last_append_ns = now_ns;
                return Aggregated{ .bytes = line, .truncated = truncated };
            },
            .pattern => return self.pushPattern(allocator, line, truncated, now_ns),
            .stanza => return self.pushStanza(allocator, line, truncated, now_ns),
        }
    }

    fn flushDueToTimeout(
        self: *MultilineState,
        now_ns: i128,
    ) PatternError!?Aggregated {
        if (!self.has_pending) return null;
        if (self.timeout_ns == 0) return null;
        if (now_ns - self.last_append_ns < self.timeout_ns) return null;
        return try self.flushPending();
    }

    fn flushPending(self: *MultilineState) PatternError!?Aggregated {
        if (!self.has_pending) return null;
        const owned = try self.buffer.toOwnedSlice();
        defer self.resetBuffer();
        return Aggregated{
            .bytes = owned,
            .truncated = self.pending_truncated,
        };
    }

    fn pushPattern(
        self: *MultilineState,
        allocator: std.mem.Allocator,
        line: []u8,
        truncated: bool,
        now_ns: i128,
    ) PatternError!?Aggregated {
        const is_start = try self.matchesStart(line);
        if (is_start) {
            const flushed = try self.flushPending();
            try self.startNew(allocator, line, truncated, now_ns);
            return flushed;
        }

        if (!self.has_pending) {
            try self.startNew(allocator, line, truncated, now_ns);
            return null;
        }

        try self.appendToPending(allocator, line, truncated);
        self.last_append_ns = now_ns;
        return null;
    }

    fn pushStanza(
        self: *MultilineState,
        allocator: std.mem.Allocator,
        line: []u8,
        truncated: bool,
        now_ns: i128,
    ) !?Aggregated {
        if (line.len == 0) {
            allocator.free(line);
            const flushed = try self.flushPending();
            return flushed;
        }

        if (!self.has_pending) {
            try self.startNew(allocator, line, truncated, now_ns);
            return null;
        }

        try self.appendToPending(allocator, line, truncated);
        self.last_append_ns = now_ns;
        return null;
    }

    fn startNew(
        self: *MultilineState,
        allocator: std.mem.Allocator,
        line: []u8,
        truncated: bool,
        now_ns: i128,
    ) !void {
        self.resetBuffer();
        const to_take = @min(line.len, self.max_bytes);
        try self.buffer.ensureTotalCapacity(self.buffer.items.len + to_take);
        try self.buffer.appendSlice(line[0..to_take]);
        allocator.free(line);
        self.pending_truncated = truncated or (to_take < line.len);
        self.has_pending = true;
        self.last_append_ns = now_ns;
    }

    fn appendToPending(
        self: *MultilineState,
        allocator: std.mem.Allocator,
        line: []u8,
        truncated: bool,
    ) !void {
        if (self.buffer.items.len != 0 and self.buffer.items.len < self.max_bytes) {
            try self.buffer.append('\n');
        }
        const remaining = if (self.buffer.items.len >= self.max_bytes) 0 else self.max_bytes - self.buffer.items.len;
        if (remaining > 0) {
            const to_take = @min(line.len, remaining);
            try self.buffer.appendSlice(line[0..to_take]);
            if (to_take < line.len) {
                self.pending_truncated = true;
            }
        } else {
            self.pending_truncated = true;
        }
        allocator.free(line);
        self.pending_truncated = self.pending_truncated or truncated;
    }

    fn resetBuffer(self: *MultilineState) void {
        self.buffer.clearRetainingCapacity();
        self.pending_truncated = false;
        self.has_pending = false;
    }

    fn matchesStart(self: *MultilineState, line: []const u8) PatternError!bool {
        const pattern = self.start_pattern orelse return true;
        if (comptime has_posix_regex) {
            if (self.compiled_regex) |*compiled| {
                return compiled.matches(self.allocator, line);
            }
        }
        return std.mem.startsWith(u8, line, pattern);
    }
};

fn patternErrorToPump(state: *StdinState, err: PatternError) PumpError {
    if (comptime has_posix_regex) {
        return switch (err) {
            error.InvalidPattern => blk: {
                logError(state, "stdin source {s}: invalid multiline regex", .{state.descriptor.name});
                break :blk PumpError.ReadFailure;
            },
            error.RegexExecFailed => blk: {
                logError(state, "stdin source {s}: regex execution failed", .{state.descriptor.name});
                break :blk PumpError.ReadFailure;
            },
            error.OutOfMemory => PumpError.Backpressure,
        };
    }
    return PumpError.Backpressure;
}

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

const metrics = struct {
    const enqueued = "sources_stdin_events_enqueued_total";
    const dropped = "sources_stdin_events_dropped_total";
    const rejected = "sources_stdin_events_rejected_total";
    const emitted = "sources_stdin_events_emitted_total";
    const truncated = "sources_stdin_events_truncated_total";
    const decode_errors = "sources_stdin_decode_errors_total";
    const read_errors = "sources_stdin_read_errors_total";
    const queue_depth = "sources_stdin_buffer_depth";
    const ack_success = "sources_stdin_ack_success_total";
    const ack_retryable = "sources_stdin_ack_retryable_total";
    const ack_failure = "sources_stdin_ack_failure_total";
};

const StdinState = struct {
    allocator: std.mem.Allocator,
    descriptor: src.SourceDescriptor,
    config: cfg.StdinConfig,
    buffer: EventBuffer,
    log: ?*const src.Logger,
    metrics: ?*const src.Metrics,
    fd: posix.fd_t,
    nonblocking: bool = false,
    eof_seen: bool = false,
    framer: frame.Framer,
    multiline: MultilineState,
    arena_pool: arena_pool.EventArenaPool,
    rate_limiter: ?RateLimiter = null,
    ready_observer: ?src.ReadyObserver = null,
    time_source: TimeSource = defaultTimeSource,
    decoder: Decoder,
    read_buffer: []u8,
    truncated_in_multiline: bool = false,
};

fn create(ctx: src.InitContext, config: *const cfg.SourceConfig) src.SourceError!src.Source {
    const stdin_cfg = switch (config.payload) {
        .stdin => |value| value,
        else => return src.SourceError.InvalidConfiguration,
    };

    const descriptor = src.SourceDescriptor{
        .type = .stdin,
        .name = config.id,
    };

    return instantiateSource(ctx, descriptor, stdin_cfg);
}

fn instantiateSource(
    ctx: src.InitContext,
    descriptor: src.SourceDescriptor,
    stdin_cfg: cfg.StdinConfig,
) src.SourceError!src.Source {
    var multiline = MultilineState.init(ctx.allocator, stdin_cfg) catch |err| {
        if (comptime has_posix_regex) {
            switch (err) {
                error.InvalidPattern => {
                    logErrorCtx(ctx.log, descriptor.name, "stdin source {s}: invalid multiline regex", .{descriptor.name});
                    return src.SourceError.StartupFailed;
                },
                error.RegexExecFailed => {
                    logErrorCtx(ctx.log, descriptor.name, "stdin source {s}: regex execution failed during init", .{descriptor.name});
                    return src.SourceError.StartupFailed;
                },
                else => {},
            }
        }
        logErrorCtx(ctx.log, descriptor.name, "stdin source {s}: failed to initialise multiline state ({s})", .{ descriptor.name, @errorName(err) });
        return src.SourceError.StartupFailed;
    };
    errdefer multiline.deinit();

    const time_source: TimeSource = defaultTimeSource;
    const now_ns = time_source();
    const rate_limiter_value = if (stdin_cfg.rate_limit_per_sec) |rate| blk: {
        const burst = stdin_cfg.rate_limit_burst orelse rate;
        break :blk RateLimiter.init(now_ns, rate, burst);
    } else null;

    const framer_mode: frame.Mode = switch (stdin_cfg.line_delimiter) {
        .auto => .auto,
        .lf => .lf,
        .crlf => .lf,
    };

    var state = ctx.allocator.create(StdinState) catch {
        multiline.deinit();
        return src.SourceError.StartupFailed;
    };
    errdefer ctx.allocator.destroy(state);

    state.* = .{
        .allocator = ctx.allocator,
        .descriptor = descriptor,
        .config = stdin_cfg,
        .buffer = undefined,
        .log = ctx.log,
        .metrics = ctx.metrics,
        .fd = posix.STDIN_FILENO,
        .nonblocking = false,
        .framer = frame.Framer.init(ctx.allocator, framer_mode, stdin_cfg.message_size_limit),
        .multiline = multiline,
        .arena_pool = arena_pool.EventArenaPool.init(ctx.allocator),
        .rate_limiter = rate_limiter_value,
        .decoder = Decoder.init(ctx.allocator, stdin_cfg),
        .read_buffer = &[_]u8{},
    };
    errdefer state.framer.deinit();
    errdefer state.multiline.deinit();
    errdefer state.arena_pool.deinit();

    state.read_buffer = ctx.allocator.alloc(u8, stdin_cfg.read_buffer_bytes) catch {
        return src.SourceError.StartupFailed;
    };
    errdefer ctx.allocator.free(state.read_buffer);

    state.buffer = EventBuffer.init(ctx.allocator, stdin_cfg.queue_capacity, stdin_cfg.when_full) catch {
        return src.SourceError.StartupFailed;
    };
    errdefer state.buffer.deinit();

    state.nonblocking = initNonblocking(state.fd);
    state.time_source = time_source;

    if (!state.nonblocking) {
        logWarn(state, "stdin source {s}: failed to enable nonblocking mode", .{descriptor.name});
    }

    const stream_lifecycle = src.StreamLifecycle{
        .start_stream = startStream,
        .shutdown = shutdown,
        .ready_hint = readyHint,
        .register_ready_observer = registerReadyObserver,
    };

    const batching = src.BatchLifecycle{
        .poll_batch = pollBatch,
        .shutdown = shutdown,
        .ready_hint = readyHint,
        .register_ready_observer = registerReadyObserver,
    };

    return src.Source{
        .stream = .{
            .descriptor = state.descriptor,
            .capabilities = .{ .streaming = true, .batching = true },
            .context = state,
            .lifecycle = stream_lifecycle,
            .batching = .{ .supported = batching },
        },
    };
}

fn initNonblocking(fd: posix.fd_t) bool {
    if (comptime @hasDecl(std.c, "O_NONBLOCK")) {
        var flags = posix.fcntl(fd, posix.F.GETFL, 0) catch return false;
        flags |= std.c.O_NONBLOCK;
        return (posix.fcntl(fd, posix.F.SETFL, flags) catch return false) == 0;
    } else {
        return false;
    }
}

fn startStream(context: *anyopaque, allocator: std.mem.Allocator) src.SourceError!?event.EventStream {
    _ = allocator;
    const state = asState(context);
    state.eof_seen = false;
    return event.EventStream{
        .context = state,
        .next_fn = streamNext,
        .finish_fn = streamFinish,
    };
}

fn streamNext(context: *anyopaque, allocator: std.mem.Allocator) event.StreamError!?event.EventBatch {
    const state = asState(context);
    if (state.eof_seen and state.buffer.isEmpty()) return event.StreamError.EndOfStream;

    pump(state) catch |err| {
        return switch (err) {
            PumpError.Backpressure => event.StreamError.Backpressure,
            PumpError.ReadFailure => event.StreamError.TransportFailure,
        };
    };

    const result = drainBatch(state, allocator) catch {
        return event.StreamError.Backpressure;
    };
    return result;
}

fn streamFinish(context: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const state = asState(context);
    state.eof_seen = true;
}

fn pollBatch(context: *anyopaque, allocator: std.mem.Allocator) src.SourceError!?event.EventBatch {
    const state = asState(context);
    pump(state) catch |err| {
        return switch (err) {
            PumpError.Backpressure => src.SourceError.Backpressure,
            PumpError.ReadFailure => src.SourceError.StartupFailed,
        };
    };

    const result = drainBatch(state, allocator) catch {
        return src.SourceError.Backpressure;
    };
    return result;
}

fn shutdown(context: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const state = asState(context);
    state.ready_observer = null;
    state.buffer.deinitWith(struct {
        fn finalize(managed: event.ManagedEvent) void {
            managed.finalizer.run();
        }
    }.finalize);
    state.framer.deinit();
    state.multiline.deinit();
    state.arena_pool.deinit();
    state.allocator.free(state.read_buffer);
    state.allocator.destroy(state);
}

fn readyHint(context: *anyopaque) bool {
    const state = asState(context);
    if (!state.buffer.isEmpty()) return true;
    return state.framer.hasBufferedData();
}

fn registerReadyObserver(context: *anyopaque, observer: src.ReadyObserver) void {
    const state = asState(context);
    state.ready_observer = observer;
    if (readyHint(context)) {
        signalReady(state);
    }
}

fn asState(ptr: *anyopaque) *StdinState {
    const aligned: *align(@alignOf(StdinState)) anyopaque = @alignCast(ptr);
    return @ptrCast(aligned);
}

const PumpError = error{
    Backpressure,
    ReadFailure,
};

fn pump(state: *StdinState) PumpError!void {
    while (true) {
        const read_len = posix.read(state.fd, state.read_buffer) catch |err| switch (err) {
            error.WouldBlock => break,
            else => {
                if (comptime std.meta.fieldIndex(@TypeOf(err), "Interrupted") != null) {
                    if (err == error.Interrupted) continue;
                }
                recordReadError(state);
                logError(state, "stdin source {s}: read failed ({s})", .{ state.descriptor.name, @errorName(err) });
                return PumpError.ReadFailure;
            },
        };

        if (read_len == 0) {
            state.eof_seen = true;
            break;
        }

        const chunk = state.read_buffer[0..read_len];
        state.framer.push(chunk) catch |err| {
            recordReject(state, 1);
            logError(state, "stdin source {s}: failed to buffer chunk ({s})", .{ state.descriptor.name, @errorName(err) });
            continue;
        };

        try drainFramedMessages(state);
    }

    const now_ns = state.time_source();

    if (state.eof_seen) {
        if (state.config.flush_partial_on_close) {
            const drained = state.framer.drainBuffered(true) catch |err| {
                recordReject(state, 1);
                logWarn(state, "stdin source {s}: failed to drain buffered data ({s})", .{ state.descriptor.name, @errorName(err) });
                return;
            };

            if (drained) |frame_result| {
                try processFrame(state, frame_result, now_ns);
            }

            if (state.multiline.flushPending() catch |err| return patternErrorToPump(state, err)) |aggregated| {
                try handleAggregated(state, aggregated, now_ns);
            }
        } else {
            const drained = state.framer.drainBuffered(false) catch null;
            if (drained) |frame_result| {
                state.allocator.free(frame_result.payload);
            }
            state.multiline.resetBuffer();
        }
    } else {
        if (state.multiline.flushDueToTimeout(now_ns) catch |err| return patternErrorToPump(state, err)) |aggregated| {
            try handleAggregated(state, aggregated, now_ns);
        }
    }
}

fn drainFramedMessages(state: *StdinState) PumpError!void {
    while (true) {
        const extracted = state.framer.next() catch |err| {
            recordReject(state, 1);
            logWarn(state, "stdin source {s}: dropped invalid frame ({s})", .{ state.descriptor.name, @errorName(err) });
            continue;
        };

        const frame_result = extracted orelse break;
        const now_ns = state.time_source();
        try processFrame(state, frame_result, now_ns);
    }
}

fn processFrame(state: *StdinState, frame_result: frame.FramerResult, now_ns: i128) PumpError!void {
    const maybe_aggregated = state.multiline.push(state.allocator, frame_result.payload, frame_result.truncated, now_ns) catch |err| {
        return patternErrorToPump(state, err);
    };
    if (maybe_aggregated) |aggregated| {
        try handleAggregated(state, aggregated, now_ns);
    }
}

fn handleAggregated(
    state: *StdinState,
    aggregated: Aggregated,
    timestamp_ns: i128,
) PumpError!void {
    defer state.allocator.free(aggregated.bytes);

    const arena = state.arena_pool.acquire() catch |err| switch (err) {
        arena_pool.EventArenaPool.Error.OutOfMemory => return PumpError.Backpressure,
    };
    var arena_owned = true;
    errdefer if (arena_owned) state.arena_pool.release(arena);

    const outcome = state.decoder.decode(state, arena, aggregated.bytes, aggregated.truncated, timestamp_ns) catch |err| switch (err) {
        DecodeError.InvalidJson => {
            logWarn(state, "stdin source {s}: failed to decode event ({s})", .{ state.descriptor.name, @errorName(err) });
            state.arena_pool.release(arena);
            arena_owned = false;
            return;
        },
        DecodeError.InvalidUtf8 => {
            recordDecodeError(state, 1);
            logWarn(state, "stdin source {s}: failed to decode event ({s})", .{ state.descriptor.name, @errorName(err) });
            state.arena_pool.release(arena);
            arena_owned = false;
            return;
        },
        error.OutOfMemory => return PumpError.Backpressure,
    };

    arena_owned = false;

    const now_ns = timestamp_ns;
    if (state.rate_limiter) |*limiter| {
        if (!limiter.allow(now_ns)) {
            recordReject(state, 1);
            logWarn(state, "stdin source {s}: rate limit exceeded", .{state.descriptor.name});
            outcome.managed.finalizer.run();
            return;
        }
    }

    if (outcome.truncated) {
        recordTruncated(state, 1);
    }

    enqueueEvent(state, outcome.managed) catch |err| switch (err) {
        src.SourceError.Backpressure => {},
        else => {
            logError(state, "stdin source {s}: enqueue failed ({s})", .{ state.descriptor.name, @errorName(err) });
            return PumpError.ReadFailure;
        },
    };
}

pub const testing = struct {
    pub fn statePointer(source_instance: *src.Source) *StdinState {
        return switch (source_instance.*) {
            .stream => |stream_source| asState(stream_source.context),
            .batch => |batch_source| asState(batch_source.context),
        };
    }

    pub fn enqueueLine(
        state: *StdinState,
        line: []const u8,
        truncated: bool,
        timestamp_ns: i128,
    ) PumpError!void {
        const copy = state.allocator.dupe(u8, line) catch return PumpError.Backpressure;
        const aggregated = Aggregated{ .bytes = copy, .truncated = truncated };
        try handleAggregated(state, aggregated, timestamp_ns);
    }

    pub fn markEof(state: *StdinState) void {
        state.eof_seen = true;
    }
};

test "stdin instantiate preserves auto delimiter for octet counted frames" {
    var ctx = src.InitContext{
        .allocator = std.testing.allocator,
        .runtime = undefined,
        .log = null,
        .metrics = null,
    };

    var stdin_cfg = cfg.StdinConfig{};
    stdin_cfg.line_delimiter = .auto;

    var source = try instantiateSource(
        ctx,
        .{ .type = .stdin, .name = "stdin_auto" },
        stdin_cfg,
    );
    defer source.shutdown(std.testing.allocator);

    const state = testing.statePointer(&source);
    try std.testing.expectEqual(frame.Mode.auto, state.framer.mode);

    try state.framer.push("5 hello");
    const maybe_frame = try state.framer.next();
    try std.testing.expect(maybe_frame != null);

    const frame_result = maybe_frame.?;
    defer ctx.allocator.free(frame_result.payload);
    try std.testing.expectEqualStrings("hello", frame_result.payload);
}

test "stdin flush_partial_on_close marks truncated EOF frame" {
    var config = cfg.StdinConfig{};
    config.flush_partial_on_close = true;
    config.multiline_mode = .disabled;

    const read_buffer_mem = try std.testing.allocator.alloc(u8, config.read_buffer_bytes);

    var state = StdinState{
        .allocator = std.testing.allocator,
        .descriptor = .{ .type = .stdin, .name = "stdin_truncated" },
        .config = config,
        .buffer = try EventBuffer.init(std.testing.allocator, config.queue_capacity, config.when_full),
        .log = null,
        .metrics = null,
        .fd = posix.STDIN_FILENO,
        .nonblocking = true,
        .eof_seen = false,
        .framer = frame.Framer.init(std.testing.allocator, .lf, config.message_size_limit),
        .multiline = try MultilineState.init(std.testing.allocator, config),
        .arena_pool = arena_pool.EventArenaPool.init(std.testing.allocator),
        .rate_limiter = null,
        .ready_observer = null,
        .time_source = defaultTimeSource,
        .decoder = Decoder.init(std.testing.allocator, config),
        .read_buffer = read_buffer_mem[0..0],
    };
    defer {
        while (state.buffer.pop()) |managed_event| {
            managed_event.finalizer.run();
        }
        state.buffer.deinit();
        state.framer.deinit();
        state.multiline.deinit();
        state.arena_pool.deinit();
        std.testing.allocator.free(read_buffer_mem);
    }

    // Preload a partial frame and trigger EOF via zero-length reads.
    try state.framer.push("partial line");

    try pump(&state);

    const popped = state.buffer.pop().?;
    defer popped.finalizer.run();
    try std.testing.expect(popped.event.metadata.payload_truncated);
}

test "stdin multiline pattern aggregates lines" {
    var config = cfg.StdinConfig{};
    config.multiline_mode = .pattern;
    config.multiline_start_pattern = "START";

    var multiline = try MultilineState.init(std.testing.allocator, config);
    defer multiline.deinit();

    const first = try std.testing.allocator.dupe(u8, "START one");
    const maybe_first = try multiline.push(std.testing.allocator, first, false, 1);
    try std.testing.expect(maybe_first == null);

    const middle = try std.testing.allocator.dupe(u8, "middle");
    const maybe_middle = try multiline.push(std.testing.allocator, middle, false, 2);
    try std.testing.expect(maybe_middle == null);

    const second = try std.testing.allocator.dupe(u8, "START two");
    const maybe_flush = try multiline.push(std.testing.allocator, second, false, 3);
    try std.testing.expect(maybe_flush != null);
    const aggregated_one = maybe_flush.?;
    defer std.testing.allocator.free(aggregated_one.bytes);
    try std.testing.expectEqualStrings("START one\nmiddle", aggregated_one.bytes);
    try std.testing.expect(!aggregated_one.truncated);

    const final_flush = try multiline.flushPending();
    try std.testing.expect(final_flush != null);
    const aggregated_two = final_flush.?;
    defer std.testing.allocator.free(aggregated_two.bytes);
    try std.testing.expectEqualStrings("START two", aggregated_two.bytes);
}

test "stdin multiline pattern matches regex" {
    var config = cfg.StdinConfig{};
    config.multiline_mode = .pattern;
    config.multiline_start_pattern = "^(INFO|WARN)";

    var multiline = try MultilineState.init(std.testing.allocator, config);
    defer multiline.deinit();

    const info = try std.testing.allocator.dupe(u8, "INFO start");
    const maybe_info = try multiline.push(std.testing.allocator, info, false, 1);
    try std.testing.expect(maybe_info == null);

    const cont = try std.testing.allocator.dupe(u8, "middle");
    const maybe_cont = try multiline.push(std.testing.allocator, cont, false, 2);
    try std.testing.expect(maybe_cont == null);

    const warn = try std.testing.allocator.dupe(u8, "WARN next");
    const flushed = try multiline.push(std.testing.allocator, warn, false, 3);
    try std.testing.expect(flushed != null);
    const aggregated_info = flushed.?;
    defer std.testing.allocator.free(aggregated_info.bytes);
    try std.testing.expectEqualStrings("INFO start\nmiddle", aggregated_info.bytes);

    const final_flush = try multiline.flushPending();
    try std.testing.expect(final_flush != null);
    const aggregated_warn = final_flush.?;
    defer std.testing.allocator.free(aggregated_warn.bytes);
    try std.testing.expectEqualStrings("WARN next", aggregated_warn.bytes);
}

test "stdin decoder ndjson fallback to text" {
    var config = cfg.StdinConfig{};
    config.codec = .ndjson;
    config.on_decode_error = .as_text;
    config.include_source_field = true;

    var state = StdinState{
        .allocator = std.testing.allocator,
        .descriptor = .{ .type = .stdin, .name = "stdin_test" },
        .config = config,
        .buffer = undefined,
        .log = null,
        .metrics = null,
        .fd = posix.STDIN_FILENO,
        .nonblocking = true,
        .eof_seen = false,
        .framer = frame.Framer.init(std.testing.allocator, .lf, config.message_size_limit),
        .multiline = try MultilineState.init(std.testing.allocator, config),
        .arena_pool = arena_pool.EventArenaPool.init(std.testing.allocator),
        .rate_limiter = null,
        .ready_observer = null,
        .time_source = defaultTimeSource,
        .decoder = Decoder.init(std.testing.allocator, config),
        .read_buffer = &[_]u8{},
    };
    defer {
        state.framer.deinit();
        state.multiline.deinit();
        state.arena_pool.deinit();
    }

    const arena = state.arena_pool.acquire() catch unreachable;
    const bytes = try std.testing.allocator.dupe(u8, "not json");
    defer std.testing.allocator.free(bytes);

    const outcome = try state.decoder.decode(&state, arena, bytes, false, 42);
    defer outcome.managed.finalizer.run();

    try std.testing.expectEqualStrings("not json", outcome.managed.event.payload.log.message);
    try std.testing.expect(!outcome.truncated);
    try std.testing.expectEqual(@as(usize, 1), outcome.managed.event.payload.log.fields.len);
    try std.testing.expectEqualStrings(SOURCE_FIELD_NAME, outcome.managed.event.payload.log.fields[0].name);
    try std.testing.expectEqualStrings(SOURCE_FIELD_VALUE, outcome.managed.event.payload.log.fields[0].value.string);
}

test "stdin decoder ndjson drop on error" {
    var config = cfg.StdinConfig{};
    config.codec = .ndjson;
    config.on_decode_error = .drop;

    var state = StdinState{
        .allocator = std.testing.allocator,
        .descriptor = .{ .type = .stdin, .name = "stdin_test" },
        .config = config,
        .buffer = undefined,
        .log = null,
        .metrics = null,
        .fd = posix.STDIN_FILENO,
        .nonblocking = true,
        .eof_seen = false,
        .framer = frame.Framer.init(std.testing.allocator, .lf, config.message_size_limit),
        .multiline = try MultilineState.init(std.testing.allocator, config),
        .arena_pool = arena_pool.EventArenaPool.init(std.testing.allocator),
        .rate_limiter = null,
        .ready_observer = null,
        .time_source = defaultTimeSource,
        .decoder = Decoder.init(std.testing.allocator, config),
        .read_buffer = &[_]u8{},
    };
    defer {
        state.framer.deinit();
        state.multiline.deinit();
        state.arena_pool.deinit();
    }

    const arena = state.arena_pool.acquire() catch unreachable;
    const bytes = try std.testing.allocator.dupe(u8, "{ invalid json");
    defer std.testing.allocator.free(bytes);

    const result = state.decoder.decode(&state, arena, bytes, false, 11);
    try std.testing.expectError(DecodeError.InvalidJson, result);
    state.arena_pool.release(arena);
}

test "stdin decoder ndjson drop counts decode error once" {
    const TestMetrics = struct {
        decode_errors: usize = 0,

        fn incr(context: *anyopaque, name: []const u8, value: u64) void {
            const self_ptr: *@This() = @ptrCast(@alignCast(context));
            if (std.mem.eql(u8, name, metrics.decode_errors)) {
                self_ptr.decode_errors += @intCast(value);
            }
        }
    };

    var config = cfg.StdinConfig{};
    config.codec = .ndjson;
    config.on_decode_error = .drop;

    var state = StdinState{
        .allocator = std.testing.allocator,
        .descriptor = .{ .type = .stdin, .name = "stdin_metric" },
        .config = config,
        .buffer = undefined,
        .log = null,
        .metrics = null,
        .fd = posix.STDIN_FILENO,
        .nonblocking = true,
        .eof_seen = false,
        .framer = frame.Framer.init(std.testing.allocator, .lf, config.message_size_limit),
        .multiline = try MultilineState.init(std.testing.allocator, config),
        .arena_pool = arena_pool.EventArenaPool.init(std.testing.allocator),
        .rate_limiter = null,
        .ready_observer = null,
        .time_source = defaultTimeSource,
        .decoder = Decoder.init(std.testing.allocator, config),
        .read_buffer = &[_]u8{},
    };
    defer {
        state.framer.deinit();
        state.multiline.deinit();
        state.arena_pool.deinit();
    }

    var metrics_state = TestMetrics{};
    var metrics_iface = src.Metrics{
        .context = &metrics_state,
        .incr_counter_fn = TestMetrics.incr,
        .record_gauge_fn = null,
    };
    state.metrics = &metrics_iface;

    const bytes = try state.allocator.dupe(u8, "{ invalid json");
    const aggregated = Aggregated{ .bytes = bytes, .truncated = false };

    try handleAggregated(&state, aggregated, 99);

    try std.testing.expectEqual(@as(usize, 1), metrics_state.decode_errors);
}

test "stdin multiline stanza flushes on blank line" {
    var config = cfg.StdinConfig{};
    config.multiline_mode = .stanza;

    var multiline = try MultilineState.init(std.testing.allocator, config);
    defer multiline.deinit();

    const first = try std.testing.allocator.dupe(u8, "line one");
    const maybe_first = try multiline.push(std.testing.allocator, first, false, 1);
    try std.testing.expect(maybe_first == null);

    const second = try std.testing.allocator.dupe(u8, "line two");
    const maybe_second = try multiline.push(std.testing.allocator, second, false, 2);
    try std.testing.expect(maybe_second == null);

    const blank = try std.testing.allocator.dupe(u8, "");
    const flushed = try multiline.push(std.testing.allocator, blank, false, 3);
    try std.testing.expect(flushed != null);
    const stanza = flushed.?;
    defer std.testing.allocator.free(stanza.bytes);
    try std.testing.expectEqualStrings("line one\nline two", stanza.bytes);
    try std.testing.expect(!stanza.truncated);
}

test "stdin multiline timeout flushes aggregate" {
    var config = cfg.StdinConfig{};
    config.multiline_mode = .pattern;
    config.multiline_start_pattern = "^first";
    config.multiline_timeout_ms = 10;

    var multiline = try MultilineState.init(std.testing.allocator, config);
    defer multiline.deinit();

    const first = try std.testing.allocator.dupe(u8, "first entry");
    const maybe_first = try multiline.push(std.testing.allocator, first, false, 1);
    try std.testing.expect(maybe_first == null);

    const maybe_timeout = try multiline.flushDueToTimeout(1 + (config.multiline_timeout_ms - 1) * std.time.ns_per_ms);
    try std.testing.expect(maybe_timeout == null);

    const timeout_ns = 1 + (@as(i128, config.multiline_timeout_ms) + 1) * std.time.ns_per_ms;
    const flushed = try multiline.flushDueToTimeout(timeout_ns);
    try std.testing.expect(flushed != null);
    const stanza = flushed.?;
    defer std.testing.allocator.free(stanza.bytes);
    try std.testing.expectEqualStrings("first entry", stanza.bytes);
}

test "stdin rate limiter drops when exceeded" {
    var config = cfg.StdinConfig{};
    config.queue_capacity = 4;
    config.when_full = .drop_oldest;
    config.rate_limit_per_sec = 1;
    config.rate_limit_burst = 1;

    var state = StdinState{
        .allocator = std.testing.allocator,
        .descriptor = .{ .type = .stdin, .name = "stdin_rate" },
        .config = config,
        .buffer = try EventBuffer.init(std.testing.allocator, config.queue_capacity, config.when_full),
        .log = null,
        .metrics = null,
        .fd = posix.STDIN_FILENO,
        .nonblocking = true,
        .eof_seen = false,
        .framer = frame.Framer.init(std.testing.allocator, .lf, config.message_size_limit),
        .multiline = try MultilineState.init(std.testing.allocator, config),
        .arena_pool = arena_pool.EventArenaPool.init(std.testing.allocator),
        .rate_limiter = RateLimiter.init(0, 1, 1),
        .ready_observer = null,
        .time_source = defaultTimeSource,
        .decoder = Decoder.init(std.testing.allocator, config),
        .read_buffer = &[_]u8{},
    };
    defer {
        while (state.buffer.pop()) |managed_event| {
            managed_event.finalizer.run();
        }
        state.buffer.deinit();
        state.framer.deinit();
        state.multiline.deinit();
        state.arena_pool.deinit();
    }

    const bytes_a = try state.allocator.dupe(u8, "message");
    const aggregated_a = Aggregated{ .bytes = bytes_a, .truncated = false };
    try handleAggregated(&state, aggregated_a, 0);
    try std.testing.expectEqual(@as(usize, 1), state.buffer.len());

    const bytes_b = try state.allocator.dupe(u8, "second");
    const aggregated_b = Aggregated{ .bytes = bytes_b, .truncated = false };
    try handleAggregated(&state, aggregated_b, 0);
    try std.testing.expectEqual(@as(usize, 1), state.buffer.len());
}

test "stdin reject policy continues pumping when buffer full" {
    var config = cfg.StdinConfig{};
    config.queue_capacity = 1;
    config.when_full = .reject;

    var state = StdinState{
        .allocator = std.testing.allocator,
        .descriptor = .{ .type = .stdin, .name = "stdin_backpressure" },
        .config = config,
        .buffer = try EventBuffer.init(std.testing.allocator, config.queue_capacity, config.when_full),
        .log = null,
        .metrics = null,
        .fd = posix.STDIN_FILENO,
        .nonblocking = true,
        .eof_seen = false,
        .framer = frame.Framer.init(std.testing.allocator, .lf, config.message_size_limit),
        .multiline = try MultilineState.init(std.testing.allocator, config),
        .arena_pool = arena_pool.EventArenaPool.init(std.testing.allocator),
        .rate_limiter = null,
        .ready_observer = null,
        .time_source = defaultTimeSource,
        .decoder = Decoder.init(std.testing.allocator, config),
        .read_buffer = &[_]u8{},
    };
    defer {
        while (state.buffer.pop()) |managed_event| {
            managed_event.finalizer.run();
        }
        state.buffer.deinit();
        state.framer.deinit();
        state.multiline.deinit();
        state.arena_pool.deinit();
    }

    const bytes_a = try state.allocator.dupe(u8, "first");
    const aggregated_a = Aggregated{ .bytes = bytes_a, .truncated = false };
    try handleAggregated(&state, aggregated_a, 0);
    try std.testing.expectEqual(@as(usize, 1), state.buffer.len());

    const bytes_b = try state.allocator.dupe(u8, "second");
    const aggregated_b = Aggregated{ .bytes = bytes_b, .truncated = false };
    try handleAggregated(&state, aggregated_b, 0);
    try std.testing.expectEqual(@as(usize, 1), state.buffer.len());

    const maybe_batch = try drainBatch(&state, std.testing.allocator);
    try std.testing.expect(maybe_batch != null);
    const batch = maybe_batch.?;
    try std.testing.expectEqual(@as(usize, 1), batch.events.len);
    try batch.ack.success();
}
fn drainBatch(state: *StdinState, allocator: std.mem.Allocator) error{Backpressure}!?event.EventBatch {
    if (state.buffer.isEmpty()) return null;

    const to_take = @min(state.config.max_batch_size, state.buffer.len());
    if (to_take == 0) return null;

    const ack_context = state.allocator.create(BatchAckContext) catch
        return error.Backpressure;

    const allocated = allocator.alloc(event.Event, to_take) catch {
        state.allocator.destroy(ack_context);
        return error.Backpressure;
    };

    const finalizers = state.allocator.alloc(event.EventFinalizer, to_take) catch {
        allocator.free(allocated);
        state.allocator.destroy(ack_context);
        return error.Backpressure;
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
                "stdin source {s}: downstream reported retryable failure",
                .{ack.descriptor_name},
            ),
            .permanent_failure => logger.errorf(
                "stdin source {s}: downstream reported permanent failure",
                .{ack.descriptor_name},
            ),
        }
    }
}

fn enqueueEvent(state: *StdinState, managed: event.ManagedEvent) src.SourceError!void {
    const result = state.buffer.push(managed) catch {
        managed.finalizer.run();
        recordReject(state, 1);
        logWarn(state, "stdin source {s}: buffer is full (reject policy)", .{state.descriptor.name});
        return src.SourceError.Backpressure;
    };

    switch (result) {
        .stored => {
            recordEnqueued(state, 1);
            signalReady(state);
        },
        .dropped_newest => {
            recordDrop(state, 1);
            managed.finalizer.run();
            logWarn(
                state,
                "stdin source {s}: drop_newest policy dropped newest event",
                .{state.descriptor.name},
            );
        },
        .dropped_oldest => |evicted| {
            recordDrop(state, 1);
            evicted.finalizer.run();
            recordEnqueued(state, 1);
            logWarn(
                state,
                "stdin source {s}: drop_oldest policy evicted oldest event",
                .{state.descriptor.name},
            );
        },
    }

    recordQueueDepth(state);
}

fn signalReady(state: *StdinState) void {
    if (state.ready_observer) |observer| {
        observer.notify();
    }
}

fn recordEnqueued(state: *StdinState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.enqueued, delta);
    }
}

fn recordDrop(state: *StdinState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.dropped, delta);
    }
}

fn recordReject(state: *StdinState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.rejected, delta);
    }
}

fn recordTruncated(state: *StdinState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.truncated, delta);
    }
}

fn recordDecodeError(state: *StdinState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.decode_errors, delta);
    }
}

fn recordReadError(state: *StdinState) void {
    if (state.metrics) |metrics_sink| {
        metrics_sink.incrCounter(metrics.read_errors, 1);
    }
}

fn recordEmitted(state: *StdinState, count: usize) void {
    if (state.metrics) |metrics_sink| {
        const delta: u64 = @intCast(count);
        metrics_sink.incrCounter(metrics.emitted, delta);
    }
}

fn recordQueueDepth(state: *StdinState) void {
    if (state.metrics) |metrics_sink| {
        const depth: i64 = @intCast(state.buffer.len());
        metrics_sink.recordGauge(metrics.queue_depth, depth);
    }
}

fn logWarn(state: *StdinState, comptime fmt: []const u8, args: anytype) void {
    if (state.log) |logger| {
        logger.warnf(fmt, args);
    }
}

fn logError(state: *StdinState, comptime fmt: []const u8, args: anytype) void {
    if (state.log) |logger| {
        logger.errorf(fmt, args);
    }
}

fn logErrorCtx(logger: ?*const src.Logger, descriptor_name: []const u8, comptime fmt: []const u8, args: anytype) void {
    if (logger) |sink| {
        sink.errorf(fmt, args);
    } else {
        _ = descriptor_name;
    }
}
