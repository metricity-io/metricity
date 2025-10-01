const std = @import("std");
const event = @import("../event.zig");
const cfg = @import("../config.zig");
const netx = @import("netx");
const transport = netx.transport;
const array_list = std.array_list;
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

const FieldList = array_list.Managed(event.Field);
const StringList = array_list.Managed([]u8);

const EventAllocation = struct {
    allocator: std.mem.Allocator,
    fields: []event.Field,
    strings: StringList,
    message_finalizer: ?transport.Finalizer,

    fn release(context: ?*anyopaque) void {
        const raw = context orelse return;
        const aligned: *align(@alignOf(EventAllocation)) anyopaque = @alignCast(raw);
        const allocation: *EventAllocation = @ptrCast(aligned);
        for (allocation.strings.items) |slice| {
            allocation.allocator.free(slice);
        }
        allocation.strings.deinit();
        allocation.allocator.free(allocation.fields);
        const message_finalizer = allocation.message_finalizer;
        allocation.allocator.destroy(allocation);
        if (message_finalizer) |finalizer| {
            finalizer.run();
        }
    }
};

pub fn parseMessage(
    allocator: std.mem.Allocator,
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
        .rfc3164 => return parseRfc3164(allocator, input, facility, severity, transport_meta, payload_truncated, message_finalizer),
        .rfc5424 => return parseRfc5424(allocator, input, facility, severity, transport_meta, payload_truncated, message_finalizer),
        .auto => {
            const attempt_5424 = parseRfc5424(allocator, input, facility, severity, transport_meta, payload_truncated, message_finalizer);
            if (attempt_5424) |result| {
                return result;
            } else |err| switch (err) {
                ParseError.UnsupportedFormat, ParseError.InvalidMessage => {},
                else => return err,
            }

            return parseRfc3164(allocator, input, facility, severity, transport_meta, payload_truncated, message_finalizer);
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
    allocator: std.mem.Allocator,
    input: []const u8,
    facility: u8,
    severity: u8,
    transport_meta: ?event.TransportMetadata,
    payload_truncated: bool,
    message_finalizer: ?transport.Finalizer,
) ParseError!ParseResult {
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

    return buildResult(
        allocator,
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
}

fn parseRfc3164(
    allocator: std.mem.Allocator,
    input: []const u8,
    facility: u8,
    severity: u8,
    transport_meta: ?event.TransportMetadata,
    payload_truncated: bool,
    message_finalizer: ?transport.Finalizer,
) ParseError!ParseResult {
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

    return buildResult(
        allocator,
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
    allocator: std.mem.Allocator,
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

    var fields = FieldList.init(allocator);
    var fields_captured = false;
    errdefer if (!fields_captured) fields.deinit();

    var string_storage = StringList.init(allocator);
    var strings_captured = false;
    errdefer if (!strings_captured) string_storage.deinit();

    fields.append(.{ .name = "syslog_facility", .value = .{ .integer = facility } }) catch return ParseError.Truncated;
    fields.append(.{ .name = "syslog_severity", .value = .{ .integer = severity } }) catch return ParseError.Truncated;
    fields.append(.{ .name = "syslog_format", .value = .{ .string = try copyString(allocator, formatName(format), &string_storage) } }) catch return ParseError.Truncated;

    if (header.timestamp.len > 0 and !isNilValue(header.timestamp)) {
        fields.append(.{ .name = "syslog_timestamp", .value = .{ .string = try copyString(allocator, header.timestamp, &string_storage) } }) catch return ParseError.Truncated;
    }
    if (normalized_timestamp) |normalized| {
        fields.append(.{ .name = "syslog_timestamp_normalized", .value = .{ .string = try copyString(allocator, normalized, &string_storage) } }) catch return ParseError.Truncated;
    }
    if (header.hostname.len > 0 and !isNilValue(header.hostname)) {
        fields.append(.{ .name = "syslog_hostname", .value = .{ .string = try copyString(allocator, header.hostname, &string_storage) } }) catch return ParseError.Truncated;
    }
    if (header.app_name.len > 0 and !isNilValue(header.app_name)) {
        fields.append(.{ .name = "syslog_app_name", .value = .{ .string = try copyString(allocator, header.app_name, &string_storage) } }) catch return ParseError.Truncated;
    }
    if (header.proc_id.len > 0 and !isNilValue(header.proc_id)) {
        fields.append(.{ .name = "syslog_proc_id", .value = .{ .string = try copyString(allocator, header.proc_id, &string_storage) } }) catch return ParseError.Truncated;
    }
    if (header.msg_id.len > 0 and !isNilValue(header.msg_id)) {
        fields.append(.{ .name = "syslog_msg_id", .value = .{ .string = try copyString(allocator, header.msg_id, &string_storage) } }) catch return ParseError.Truncated;
    }
    if (header.structured_data.len > 0 and !isNilValue(header.structured_data)) {
        fields.append(.{ .name = "syslog_structured_data", .value = .{ .string = try copyString(allocator, header.structured_data, &string_storage) } }) catch return ParseError.Truncated;
    }
    if (format == .rfc5424) {
        fields.append(.{ .name = "syslog_version", .value = .{ .integer = header.version } }) catch return ParseError.Truncated;
    }
    if (!message_utf8_valid) {
        fields.append(.{ .name = "syslog_msg_utf8_valid", .value = .{ .boolean = false } }) catch return ParseError.Truncated;
    }

    const fields_slice = fields.toOwnedSlice() catch return ParseError.Truncated;
    fields_captured = true;

    const allocation = allocator.create(EventAllocation) catch return ParseError.Truncated;
    allocation.allocator = allocator;
    allocation.fields = fields_slice;
    allocation.strings = string_storage;
    allocation.message_finalizer = message_finalizer;
    strings_captured = true;

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

fn copyString(allocator: std.mem.Allocator, source: []const u8, storage: *StringList) ParseError![]const u8 {
    const copy = allocator.alloc(u8, source.len) catch return ParseError.Truncated;
    @memcpy(copy, source);
    storage.append(copy) catch return ParseError.Truncated;
    return copy;
}

fn formatName(format: FormatKind) []const u8 {
    return switch (format) {
        .rfc3164 => "rfc3164",
        .rfc5424 => "rfc5424",
    };
}
