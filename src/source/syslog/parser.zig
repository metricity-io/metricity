const std = @import("std");
const event = @import("../event.zig");
const cfg = @import("../config.zig");
const netx = @import("netx");
const transport = netx.transport;
const unicode = std.unicode;
const time = std.time;
const epoch = std.time.epoch;
const ascii = std.ascii;

const UTF8_BOM = [_]u8{ 0xEF, 0xBB, 0xBF };

pub const ParseError = error{
    InvalidMessage,
    UnsupportedFormat,
    Truncated,
    NotImplemented,
};

pub const ParseResult = struct {
    managed: event.ManagedEvent,
};

const EventArena = struct {
    arena: std.heap.ArenaAllocator,
    next: ?*EventArena = null,

    fn allocator(self: *EventArena) std.mem.Allocator {
        return self.arena.allocator();
    }
};

pub const EventArenaPool = struct {
    allocator: std.mem.Allocator,
    free_list: ?*EventArena = null,

    pub fn init(allocator: std.mem.Allocator) EventArenaPool {
        return .{ .allocator = allocator, .free_list = null };
    }

    pub fn deinit(self: *EventArenaPool) void {
        var cursor = self.free_list;
        while (cursor) |arena| {
            const next = arena.next;
            arena.arena.deinit();
            self.allocator.destroy(arena);
            cursor = next;
        }
        self.free_list = null;
    }

    pub fn acquire(self: *EventArenaPool) ParseError!*EventArena {
        if (self.free_list) |arena| {
            self.free_list = arena.next;
            arena.next = null;
            return arena;
        }

        const arena = self.allocator.create(EventArena) catch return ParseError.Truncated;
        arena.* = .{
            .arena = std.heap.ArenaAllocator.init(self.allocator),
            .next = null,
        };
        return arena;
    }

    pub fn release(self: *EventArenaPool, arena: *EventArena) void {
        _ = arena.arena.reset(.retain_capacity);
        arena.next = self.free_list;
        self.free_list = arena;
    }

    pub fn freeCount(self: *const EventArenaPool) usize {
        var cursor = self.free_list;
        var count: usize = 0;
        while (cursor) |arena| {
            count += 1;
            cursor = arena.next;
        }
        return count;
    }
};

const EventAllocation = struct {
    pool: *EventArenaPool,
    arena: *EventArena,
    message_finalizer: ?transport.Finalizer,

    fn release(context: ?*anyopaque) void {
        const raw = context orelse return;
        const aligned: *align(@alignOf(EventAllocation)) anyopaque = @alignCast(raw);
        const allocation: *EventAllocation = @ptrCast(aligned);

        allocation.pool.release(allocation.arena);

        const message_finalizer = allocation.message_finalizer;
        allocation.pool.allocator.destroy(allocation);

        if (message_finalizer) |finalizer| {
            finalizer.run();
        }
    }
};

pub fn parseMessage(
    pool: *EventArenaPool,
    config: cfg.SyslogConfig,
    message: transport.Message,
) ParseError!ParseResult {
    var input = std.mem.trim(u8, message.bytes, " \r\n");
    if (input.len == 0) return ParseError.InvalidMessage;
    const pr = try parsePriority(input);
    const facility = pr.facility;
    const severity = pr.severity;
    input = pr.rest;

    const message_finalizer = message.finalizer;
    const transport_meta = buildTransportMetadata(message.metadata);
    const payload_truncated = message.metadata.truncated;

    switch (config.parser) {
        .rfc3164 => return parseRfc3164(pool, input, facility, severity, transport_meta, payload_truncated, message_finalizer),
        .rfc5424 => return parseRfc5424(pool, input, facility, severity, transport_meta, payload_truncated, message_finalizer),
        .auto => {
            const attempt_5424 = parseRfc5424(pool, input, facility, severity, transport_meta, payload_truncated, message_finalizer);
            if (attempt_5424) |result| {
                return result;
            } else |err| switch (err) {
                ParseError.UnsupportedFormat, ParseError.InvalidMessage => {},
                else => return err,
            }

            return parseRfc3164(pool, input, facility, severity, transport_meta, payload_truncated, message_finalizer);
        },
    }
}

const PriorityParse = struct {
    facility: u8,
    severity: u8,
    rest: []const u8,
};

fn parsePriority(input: []const u8) ParseError!PriorityParse {
    if (input.len < 3 or input[0] != '<') return ParseError.InvalidMessage;
    var idx: usize = 1;
    var value: usize = 0;
    while (idx < input.len and input[idx] != '>') : (idx += 1) {
        const ch = input[idx];
        if (ch < '0' or ch > '9') return ParseError.InvalidMessage;
        value = std.math.mul(usize, value, 10) catch return ParseError.InvalidMessage;
        value = std.math.add(usize, value, ch - '0') catch return ParseError.InvalidMessage;
    }
    if (idx == input.len or input[idx] != '>') return ParseError.InvalidMessage;
    if (value > 191) return ParseError.InvalidMessage;
    const pri = value;
    const facility: u8 = @intCast(pri / 8);
    const severity: u8 = @intCast(pri % 8);
    const rest = std.mem.trimLeft(u8, input[idx + 1 ..], " ");
    if (rest.len == 0) return ParseError.InvalidMessage;
    return PriorityParse{ .facility = facility, .severity = severity, .rest = rest };
}

fn parseRfc5424(
    pool: *EventArenaPool,
    input: []const u8,
    facility: u8,
    severity: u8,
    transport_meta: ?event.TransportMetadata,
    payload_truncated: bool,
    message_finalizer: ?transport.Finalizer,
) ParseError!ParseResult {
    var arena = try pool.acquire();
    var arena_owned = true;
    errdefer if (arena_owned) pool.release(arena);

    var cursor = input;
    const version_end = std.mem.indexOfScalar(u8, cursor, ' ') orelse return ParseError.InvalidMessage;
    const version_slice = cursor[0..version_end];
    const version = std.fmt.parseInt(u8, version_slice, 10) catch return ParseError.InvalidMessage;
    if (version != 1) return ParseError.UnsupportedFormat;
    cursor = std.mem.trimLeft(u8, cursor[version_end + 1 ..], " ");

    const timestamp_token = nextToken(&cursor) orelse return ParseError.InvalidMessage;
    const hostname_token = nextToken(&cursor) orelse return ParseError.InvalidMessage;
    const app_name_token = nextToken(&cursor) orelse return ParseError.InvalidMessage;
    const procid_token = nextToken(&cursor) orelse return ParseError.InvalidMessage;
    const msgid_token = nextToken(&cursor) orelse return ParseError.InvalidMessage;

    const sd_extract = extractStructuredData(cursor) catch return ParseError.InvalidMessage;
    const structured_data = sd_extract.data;
    cursor = std.mem.trimLeft(u8, sd_extract.rest, " ");

    var message_text = cursor;
    if (message_text.len >= UTF8_BOM.len and std.mem.startsWith(u8, message_text, &UTF8_BOM)) {
        message_text = message_text[UTF8_BOM.len..];
    }
    const message_is_utf8 = unicode.utf8ValidateSlice(message_text);

    const result = try buildResult(
        pool,
        arena,
        arena.allocator(),
        facility,
        severity,
        message_text,
        message_is_utf8,
        .{
            .timestamp = timestamp_token,
            .hostname = hostname_token,
            .app_name = app_name_token,
            .proc_id = procid_token,
            .msg_id = msgid_token,
            .structured_data = structured_data,
            .version = version,
        },
        .rfc5424,
        null,
        transport_meta,
        payload_truncated,
        message_finalizer,
    );

    arena_owned = false;
    return result;
}

fn parseRfc3164(
    pool: *EventArenaPool,
    input: []const u8,
    facility: u8,
    severity: u8,
    transport_meta: ?event.TransportMetadata,
    payload_truncated: bool,
    message_finalizer: ?transport.Finalizer,
) ParseError!ParseResult {
    var arena = try pool.acquire();
    var arena_owned = true;
    errdefer if (arena_owned) pool.release(arena);

    var cursor = input;
    const timestamp_len = findTimestampEnd(cursor) orelse return ParseError.InvalidMessage;
    const timestamp = cursor[0..timestamp_len];
    cursor = std.mem.trimLeft(u8, cursor[timestamp_len..], " ");
    const hostname_end = std.mem.indexOfScalar(u8, cursor, ' ') orelse return ParseError.InvalidMessage;
    const hostname = cursor[0..hostname_end];
    cursor = std.mem.trimLeft(u8, cursor[hostname_end + 1 ..], " ");

    var tag: []const u8 = "";
    var message_body = cursor;
    if (std.mem.indexOfScalar(u8, cursor, ':')) |colon_idx| {
        tag = std.mem.trimRight(u8, cursor[0..colon_idx], " ");
        message_body = std.mem.trimLeft(u8, cursor[colon_idx + 1 ..], " ");
    }

    var normalized_storage: [32]u8 = undefined;
    const normalized_ts = normalizeRfc3164Timestamp(timestamp, normalized_storage[0..]);

    const result = try buildResult(
        pool,
        arena,
        arena.allocator(),
        facility,
        severity,
        message_body,
        true,
        .{
            .timestamp = timestamp,
            .hostname = hostname,
            .app_name = tag,
            .proc_id = "",
            .msg_id = "",
            .structured_data = "",
            .version = 0,
        },
        .rfc3164,
        normalized_ts,
        transport_meta,
        payload_truncated,
        message_finalizer,
    );

    arena_owned = false;
    return result;
}

const FormatKind = enum { rfc3164, rfc5424 };

const HeaderInfo = struct {
    timestamp: []const u8,
    hostname: []const u8,
    app_name: []const u8,
    proc_id: []const u8,
    msg_id: []const u8,
    structured_data: []const u8,
    version: u8,
};

fn buildResult(
    pool: *EventArenaPool,
    arena: *EventArena,
    arena_allocator: std.mem.Allocator,
    facility: u8,
    severity: u8,
    message_text: []const u8,
    message_utf8_valid: bool,
    header: HeaderInfo,
    format: FormatKind,
    normalized_timestamp: ?[]const u8,
    transport_meta: ?event.TransportMetadata,
    payload_truncated: bool,
    message_finalizer: ?transport.Finalizer,
) ParseError!ParseResult {

    const include_timestamp = header.timestamp.len > 0 and !isNilValue(header.timestamp);
    const include_hostname = header.hostname.len > 0 and !isNilValue(header.hostname);
    const include_app = header.app_name.len > 0 and !isNilValue(header.app_name);
    const include_proc = header.proc_id.len > 0 and !isNilValue(header.proc_id);
    const include_msg_id = header.msg_id.len > 0 and !isNilValue(header.msg_id);
    const include_structured = header.structured_data.len > 0 and !isNilValue(header.structured_data);
    const include_version = format == .rfc5424;
    const include_utf8_flag = !message_utf8_valid;

    var normalized_owned: ?[]const u8 = null;
    if (normalized_timestamp) |normalized| {
        const copy = arena_allocator.alloc(u8, normalized.len) catch return ParseError.Truncated;
        @memcpy(copy, normalized);
        normalized_owned = copy;
    }

    var field_count: usize = 3; // facility, severity, format
    if (include_timestamp) field_count += 1;
    if (normalized_owned != null) field_count += 1;
    if (include_hostname) field_count += 1;
    if (include_app) field_count += 1;
    if (include_proc) field_count += 1;
    if (include_msg_id) field_count += 1;
    if (include_structured) field_count += 1;
    if (include_version) field_count += 1;
    if (include_utf8_flag) field_count += 1;

    const fields_slice = arena_allocator.alloc(event.Field, field_count) catch return ParseError.Truncated;

    var index: usize = 0;
    fields_slice[index] = .{ .name = "syslog_facility", .value = .{ .integer = facility } };
    index += 1;
    fields_slice[index] = .{ .name = "syslog_severity", .value = .{ .integer = severity } };
    index += 1;
    fields_slice[index] = .{ .name = "syslog_format", .value = .{ .string = formatName(format) } };
    index += 1;

    if (include_timestamp) {
        fields_slice[index] = .{ .name = "syslog_timestamp", .value = .{ .string = header.timestamp } };
        index += 1;
    }
    if (normalized_owned) |normalized| {
        fields_slice[index] = .{ .name = "syslog_timestamp_normalized", .value = .{ .string = normalized } };
        index += 1;
    }
    if (include_hostname) {
        fields_slice[index] = .{ .name = "syslog_hostname", .value = .{ .string = header.hostname } };
        index += 1;
    }
    if (include_app) {
        fields_slice[index] = .{ .name = "syslog_app_name", .value = .{ .string = header.app_name } };
        index += 1;
    }
    if (include_proc) {
        fields_slice[index] = .{ .name = "syslog_proc_id", .value = .{ .string = header.proc_id } };
        index += 1;
    }
    if (include_msg_id) {
        fields_slice[index] = .{ .name = "syslog_msg_id", .value = .{ .string = header.msg_id } };
        index += 1;
    }
    if (include_structured) {
        fields_slice[index] = .{ .name = "syslog_structured_data", .value = .{ .string = header.structured_data } };
        index += 1;
    }
    if (include_version) {
        fields_slice[index] = .{ .name = "syslog_version", .value = .{ .integer = header.version } };
        index += 1;
    }
    if (include_utf8_flag) {
        fields_slice[index] = .{ .name = "syslog_msg_utf8_valid", .value = .{ .boolean = false } };
        index += 1;
    }

    std.debug.assert(index == field_count);

    const log_event = event.LogEvent{
        .message = message_text,
        .fields = fields_slice,
    };
    const ev = event.Event{
        .metadata = .{
            .transport = transport_meta,
            .payload_truncated = payload_truncated,
        },
        .payload = .{ .log = log_event },
    };

    const allocation = pool.allocator.create(EventAllocation) catch return ParseError.Truncated;
    allocation.* = .{
        .pool = pool,
        .arena = arena,
        .message_finalizer = message_finalizer,
    };

    const managed = event.ManagedEvent.init(ev, event.EventFinalizer.init(EventAllocation.release, allocation));
    return ParseResult{ .managed = managed };
}

fn buildTransportMetadata(meta: transport.Metadata) ?event.TransportMetadata {
    if (meta.peer_address.len == 0 and meta.protocol.len == 0) return null;
    return event.TransportMetadata{
        .socket = .{
            .peer_address = meta.peer_address,
            .protocol = meta.protocol,
        },
    };
}

fn nextToken(cursor: *[]const u8) ?[]const u8 {
    if (cursor.*.len == 0) return null;
    const idx = std.mem.indexOfScalar(u8, cursor.*, ' ') orelse {
        const token = cursor.*;
        cursor.* = &[_]u8{};
        return token;
    };
    const token = cursor.*[0..idx];
    cursor.* = std.mem.trimLeft(u8, cursor.*[idx + 1 ..], " ");
    return token;
}

fn extractStructuredData(cursor: []const u8) ParseError!struct {
    data: []const u8,
    rest: []const u8,
} {
    if (cursor.len == 0) return .{ .data = "", .rest = cursor };
    if (cursor[0] == '-') {
        const rest = if (cursor.len <= 1) &[_]u8{} else cursor[1..];
        return .{ .data = "", .rest = rest };
    }
    if (cursor[0] != '[') return ParseError.UnsupportedFormat;

    var depth: usize = 0;
    var in_quotes = false;
    var escape = false;
    var index: usize = 0;
    while (index < cursor.len) : (index += 1) {
        const ch = cursor[index];
        if (escape) {
            escape = false;
            continue;
        }
        if (in_quotes) {
            if (ch == '\\') {
                escape = true;
                continue;
            }
            if (ch == '"') {
                in_quotes = false;
            }
            continue;
        }

        switch (ch) {
            '[' => depth += 1,
            ']' => {
                if (depth == 0) return ParseError.InvalidMessage;
                depth -= 1;
                const next_index = index + 1;
                if (depth == 0) {
                    if (next_index >= cursor.len or cursor[next_index] == ' ') {
                        const data = cursor[0..next_index];
                        const rest = if (next_index >= cursor.len) cursor[cursor.len..] else cursor[next_index..];
                        return .{ .data = data, .rest = rest };
                    }
                }
            },
            '"' => in_quotes = true,
            '\\' => {},
            else => {},
        }
    }
    return ParseError.Truncated;
}

fn findTimestampEnd(input: []const u8) ?usize {
    var cursor = input;
    _ = nextToken(&cursor) orelse return null; // month
    _ = nextToken(&cursor) orelse return null; // day
    const time_token = nextToken(&cursor) orelse return null;
    const base_ptr = @intFromPtr(input.ptr);
    const end_ptr = @intFromPtr(time_token.ptr) + time_token.len;
    return end_ptr - base_ptr;
}

fn normalizeRfc3164Timestamp(raw: []const u8, buffer: []u8) ?[]const u8 {
    var tokenizer = std.mem.tokenizeScalar(u8, raw, ' ');
    const month_token = tokenizer.next() orelse return null;
    const day_token = tokenizer.next() orelse return null;
    const time_token = tokenizer.next() orelse return null;

    const month = parseMonthToken(month_token) orelse return null;
    const day = parseUnsigned(day_token, 1, 31) orelse return null;

    var time_parts = std.mem.tokenizeScalar(u8, time_token, ':');
    const hour_token = time_parts.next() orelse return null;
    const minute_token = time_parts.next() orelse return null;
    const second_token = time_parts.next() orelse return null;
    if (time_parts.next() != null) return null;

    const hour = parseUnsigned(hour_token, 0, 23) orelse return null;
    const minute = parseUnsigned(minute_token, 0, 59) orelse return null;
    const second = parseUnsigned(second_token, 0, 59) orelse return null;

    const now = time.timestamp();
    if (now < 0) return null;
    const now_u64: u64 = @intCast(now);
    const epoch_seconds = epoch.EpochSeconds{ .secs = now_u64 };
    const current_day = epoch_seconds.getEpochDay();
    const year_day = current_day.calculateYearDay();
    const current_month_day = year_day.calculateMonthDay();
    const current_month = current_month_day.month.numeric();
    var year: i32 = @intCast(year_day.year);

    const month_diff = @as(i32, month) - @as(i32, current_month);
    if (month_diff > 6) {
        year -= 1;
    } else if (month_diff < -6) {
        year += 1;
    }
    if (year < 0) return null;

    const result = std.fmt.bufPrint(buffer, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        year,
        month,
        day,
        hour,
        minute,
        second,
    }) catch return null;
    return result;
}

fn parseMonthToken(token: []const u8) ?u8 {
    if (token.len != 3) return null;
    var lower: [3]u8 = undefined;
    for (token, 0..) |ch, idx| {
        lower[idx] = ascii.toLower(ch);
    }
    const slice: []const u8 = lower[0..];
    if (std.mem.eql(u8, slice, "jan")) return 1;
    if (std.mem.eql(u8, slice, "feb")) return 2;
    if (std.mem.eql(u8, slice, "mar")) return 3;
    if (std.mem.eql(u8, slice, "apr")) return 4;
    if (std.mem.eql(u8, slice, "may")) return 5;
    if (std.mem.eql(u8, slice, "jun")) return 6;
    if (std.mem.eql(u8, slice, "jul")) return 7;
    if (std.mem.eql(u8, slice, "aug")) return 8;
    if (std.mem.eql(u8, slice, "sep")) return 9;
    if (std.mem.eql(u8, slice, "oct")) return 10;
    if (std.mem.eql(u8, slice, "nov")) return 11;
    if (std.mem.eql(u8, slice, "dec")) return 12;
    return null;
}

fn parseUnsigned(token: []const u8, min: u8, max: u8) ?u8 {
    const value = std.fmt.parseInt(u8, token, 10) catch return null;
    if (value < min or value > max) return null;
    return value;
}

fn isNilValue(token: []const u8) bool {
    return token.len == 1 and token[0] == '-';
}

fn formatName(format: FormatKind) []const u8 {
    return switch (format) {
        .rfc3164 => "rfc3164",
        .rfc5424 => "rfc5424",
    };
}
