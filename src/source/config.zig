const std = @import("std");
const src = @import("source.zig");

pub const ParseError = error{
    InvalidType,
    InvalidValue,
    MissingField,
    UnsupportedSource,
    UnknownField,
    Unimplemented,
};

pub const SourceConfig = struct {
    id: []const u8,
    payload: Payload,
};

pub const Payload = union(src.SourceType) {
    syslog: SyslogConfig,
};

pub const SyslogParserMode = enum {
    auto,
    rfc3164,
    rfc5424,
};

pub const SyslogTransport = enum {
    udp,
    tcp,
};

pub const SyslogConfig = struct {
    address: []const u8,
    transport: SyslogTransport = .udp,
    parser: SyslogParserMode = .auto,
    max_batch_size: usize = 256,
    read_buffer_bytes: usize = 64 * 1024,
    tcp_max_connections: usize = 64,
    tcp_keepalive_seconds: ?u32 = null,
    message_size_limit: usize = 64 * 1024,
    tcp_high_watermark: usize = 256 * 1024,
    tcp_low_watermark: usize = 128 * 1024,
    allowed_peers: []const []const u8 = &[_][]const u8{},
    rate_limit_per_sec: ?usize = null,
    rate_limit_burst: ?usize = null,
};

pub const TomlValue = union(enum) {
    string: []const u8,
    integer: i64,
    bool: bool,
    string_array: []const []const u8,
};

pub const TomlKeyValue = struct {
    key: []const u8,
    value: TomlValue,
};

/// Minimal representation used for unit tests. The actual parser can project
/// into this structure as an intermediate step until a richer API is needed.
pub const TomlTable = struct {
    entries: []const TomlKeyValue,

    pub fn get(self: TomlTable, key: []const u8) ?TomlValue {
        for (self.entries) |entry| {
            if (std.mem.eql(u8, entry.key, key)) return entry.value;
        }
        return null;
    }

    pub fn getString(self: TomlTable, key: []const u8) ParseError!?[]const u8 {
        const value = self.get(key) orelse return null;
        return switch (value) {
            .string => |s| s,
            else => ParseError.InvalidType,
        };
    }

    pub fn getInteger(self: TomlTable, key: []const u8) ParseError!?i64 {
        const value = self.get(key) orelse return null;
        return switch (value) {
            .integer => |n| n,
            else => ParseError.InvalidType,
        };
    }

    pub fn getStringArray(self: TomlTable, key: []const u8) ParseError!?[]const []const u8 {
        const value = self.get(key) orelse return null;
        return switch (value) {
            .string_array => |entries| entries,
            else => ParseError.InvalidType,
        };
    }
};

fn parseSourceType(name: []const u8) ParseError!src.SourceType {
    if (std.mem.eql(u8, name, "syslog")) return .syslog;
    return ParseError.UnsupportedSource;
}

fn parseSyslogTransport(name: []const u8) ParseError!SyslogTransport {
    if (std.mem.eql(u8, name, "udp")) return .udp;
    if (std.mem.eql(u8, name, "tcp")) return .tcp;
    return ParseError.InvalidValue;
}

fn parseSyslogParser(name: []const u8) ParseError!SyslogParserMode {
    if (std.mem.eql(u8, name, "auto")) return .auto;
    if (std.mem.eql(u8, name, "rfc3164")) return .rfc3164;
    if (std.mem.eql(u8, name, "rfc5424")) return .rfc5424;
    return ParseError.InvalidValue;
}

fn ensureKnownKeys(table: TomlTable, allowed: []const []const u8) ParseError!void {
    outer: for (table.entries) |entry| {
        for (allowed) |key| {
            if (std.mem.eql(u8, entry.key, key)) continue :outer;
        }
        return ParseError.UnknownField;
    }
}

pub fn parseSourceConfig(
    allocator: std.mem.Allocator,
    id: []const u8,
    table: *const TomlTable,
) ParseError!SourceConfig {
    const type_str = try table.getString("type") orelse return ParseError.MissingField;
    const source_type = try parseSourceType(type_str);

    switch (source_type) {
        .syslog => {
            const allowed = [_][]const u8{
                "type",
                "address",
                "transport",
                "parser",
                "max_batch_size",
                "read_buffer_bytes",
                "tcp_max_connections",
                "tcp_keepalive_seconds",
                "message_size_limit",
                "tcp_high_watermark",
                "tcp_low_watermark",
                "allowed_peers",
                "rate_limit_per_sec",
                "rate_limit_burst",
            };
            try ensureKnownKeys(table.*, &allowed);

            const address = try table.getString("address") orelse return ParseError.MissingField;

            var config = SyslogConfig{
                .address = address,
            };

            if (try table.getString("transport")) |transport_name| {
                config.transport = try parseSyslogTransport(transport_name);
            }

            if (try table.getString("parser")) |parser_name| {
                config.parser = try parseSyslogParser(parser_name);
            }

            if (try table.getInteger("max_batch_size")) |max_batch_size| {
                if (max_batch_size <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, max_batch_size) orelse return ParseError.InvalidValue;
                config.max_batch_size = casted;
            }

            if (try table.getInteger("read_buffer_bytes")) |read_buffer_bytes| {
                if (read_buffer_bytes <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, read_buffer_bytes) orelse return ParseError.InvalidValue;
                config.read_buffer_bytes = casted;
            }

            if (try table.getInteger("tcp_max_connections")) |tcp_max_connections| {
                if (tcp_max_connections <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, tcp_max_connections) orelse return ParseError.InvalidValue;
                config.tcp_max_connections = casted;
            }

            if (try table.getInteger("tcp_keepalive_seconds")) |tcp_keepalive_seconds| {
                if (tcp_keepalive_seconds <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(u32, tcp_keepalive_seconds) orelse return ParseError.InvalidValue;
                config.tcp_keepalive_seconds = casted;
            }

            if (try table.getInteger("message_size_limit")) |message_size_limit| {
                if (message_size_limit <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, message_size_limit) orelse return ParseError.InvalidValue;
                config.message_size_limit = casted;
            }

            if (try table.getInteger("tcp_high_watermark")) |tcp_high_watermark| {
                if (tcp_high_watermark <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, tcp_high_watermark) orelse return ParseError.InvalidValue;
                config.tcp_high_watermark = casted;
            }

            if (try table.getInteger("tcp_low_watermark")) |tcp_low_watermark| {
                if (tcp_low_watermark <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, tcp_low_watermark) orelse return ParseError.InvalidValue;
                config.tcp_low_watermark = casted;
            }

            if (config.tcp_low_watermark > config.tcp_high_watermark) return ParseError.InvalidValue;

            if (try table.getStringArray("allowed_peers")) |peers| {
                config.allowed_peers = peers;
            }

            if (try table.getInteger("rate_limit_per_sec")) |rate_limit| {
                if (rate_limit <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, rate_limit) orelse return ParseError.InvalidValue;
                config.rate_limit_per_sec = casted;
            }

            if (try table.getInteger("rate_limit_burst")) |burst| {
                if (burst <= 0) return ParseError.InvalidValue;
                const casted = std.math.cast(usize, burst) orelse return ParseError.InvalidValue;
                config.rate_limit_burst = casted;
            }

            if (config.rate_limit_burst != null and config.rate_limit_per_sec == null) {
                return ParseError.InvalidValue;
            }

            _ = allocator; // reserved for future allocations.
            return SourceConfig{
                .id = id,
                .payload = .{ .syslog = config },
            };
        },
    }
}

test "parse syslog config success" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "127.0.0.1:514" } },
            .{ .key = "transport", .value = TomlValue{ .string = "tcp" } },
            .{ .key = "parser", .value = TomlValue{ .string = "rfc5424" } },
            .{ .key = "max_batch_size", .value = TomlValue{ .integer = 512 } },
            .{ .key = "read_buffer_bytes", .value = TomlValue{ .integer = 131072 } },
            .{ .key = "tcp_max_connections", .value = TomlValue{ .integer = 32 } },
            .{ .key = "tcp_keepalive_seconds", .value = TomlValue{ .integer = 45 } },
            .{ .key = "message_size_limit", .value = TomlValue{ .integer = 262144 } },
            .{ .key = "tcp_high_watermark", .value = TomlValue{ .integer = 600000 } },
            .{ .key = "tcp_low_watermark", .value = TomlValue{ .integer = 200000 } },
        },
    };

    const config = try parseSourceConfig(std.testing.allocator, "syslog_1", &table);
    try std.testing.expectEqualStrings("syslog_1", config.id);
    const payload = config.payload;
    switch (payload) {
        .syslog => |syslog_cfg| {
            try std.testing.expectEqualStrings("127.0.0.1:514", syslog_cfg.address);
            try std.testing.expectEqual(SyslogTransport.tcp, syslog_cfg.transport);
            try std.testing.expectEqual(SyslogParserMode.rfc5424, syslog_cfg.parser);
            try std.testing.expectEqual(@as(usize, 512), syslog_cfg.max_batch_size);
            try std.testing.expectEqual(@as(usize, 131072), syslog_cfg.read_buffer_bytes);
            try std.testing.expectEqual(@as(usize, 32), syslog_cfg.tcp_max_connections);
            try std.testing.expectEqual(@as(?u32, 45), syslog_cfg.tcp_keepalive_seconds);
            try std.testing.expectEqual(@as(usize, 262144), syslog_cfg.message_size_limit);
            try std.testing.expectEqual(@as(usize, 600000), syslog_cfg.tcp_high_watermark);
            try std.testing.expectEqual(@as(usize, 200000), syslog_cfg.tcp_low_watermark);
        },
    }
}

test "parse syslog config applies defaults" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "udp://0.0.0.0:1514" } },
        },
    };

    const config = try parseSourceConfig(std.testing.allocator, "syslog_default", &table);
    switch (config.payload) {
        .syslog => |syslog_cfg| {
            try std.testing.expectEqualStrings("udp://0.0.0.0:1514", syslog_cfg.address);
            try std.testing.expectEqual(SyslogTransport.udp, syslog_cfg.transport);
            try std.testing.expectEqual(SyslogParserMode.auto, syslog_cfg.parser);
            try std.testing.expectEqual(@as(usize, 256), syslog_cfg.max_batch_size);
            try std.testing.expectEqual(@as(usize, 64 * 1024), syslog_cfg.read_buffer_bytes);
            try std.testing.expectEqual(@as(usize, 64), syslog_cfg.tcp_max_connections);
            try std.testing.expectEqual(@as(?u32, null), syslog_cfg.tcp_keepalive_seconds);
            try std.testing.expectEqual(@as(usize, 64 * 1024), syslog_cfg.message_size_limit);
            try std.testing.expectEqual(@as(usize, 256 * 1024), syslog_cfg.tcp_high_watermark);
            try std.testing.expectEqual(@as(usize, 128 * 1024), syslog_cfg.tcp_low_watermark);
        },
    }
}

test "parse syslog config rejects unknown keys" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "127.0.0.1:514" } },
            .{ .key = "unexpected", .value = TomlValue{ .string = "value" } },
        },
    };

    try std.testing.expectError(ParseError.UnknownField, parseSourceConfig(std.testing.allocator, "syslog_2", &table));
}

test "parse syslog config requires mandatory fields" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
        },
    };

    try std.testing.expectError(ParseError.MissingField, parseSourceConfig(std.testing.allocator, "syslog_missing", &table));
}

test "parse source config rejects invalid values" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "127.0.0.1:514" } },
            .{ .key = "transport", .value = TomlValue{ .string = "air" } },
        },
    };

    try std.testing.expectError(ParseError.InvalidValue, parseSourceConfig(std.testing.allocator, "syslog_invalid", &table));
}

test "parse syslog config rejects non-positive tcp_max_connections" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "127.0.0.1:514" } },
            .{ .key = "tcp_max_connections", .value = TomlValue{ .integer = 0 } },
        },
    };

    try std.testing.expectError(ParseError.InvalidValue, parseSourceConfig(std.testing.allocator, "syslog_tcp", &table));
}

test "parse syslog config rejects non-positive tcp_keepalive_seconds" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "127.0.0.1:514" } },
            .{ .key = "tcp_keepalive_seconds", .value = TomlValue{ .integer = -5 } },
        },
    };

    try std.testing.expectError(ParseError.InvalidValue, parseSourceConfig(std.testing.allocator, "syslog_keepalive", &table));
}

test "parse syslog config rejects non-positive message_size_limit" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "127.0.0.1:514" } },
            .{ .key = "message_size_limit", .value = TomlValue{ .integer = 0 } },
        },
    };

    try std.testing.expectError(ParseError.InvalidValue, parseSourceConfig(std.testing.allocator, "syslog_limit", &table));
}

test "parse syslog config rejects watermarks order" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .string = "127.0.0.1:514" } },
            .{ .key = "tcp_high_watermark", .value = TomlValue{ .integer = 1000 } },
            .{ .key = "tcp_low_watermark", .value = TomlValue{ .integer = 2000 } },
        },
    };

    try std.testing.expectError(ParseError.InvalidValue, parseSourceConfig(std.testing.allocator, "syslog_wm", &table));
}

test "parse source config rejects unsupported type" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "kafka" } },
            .{ .key = "address", .value = TomlValue{ .string = "localhost" } },
        },
    };

    try std.testing.expectError(ParseError.UnsupportedSource, parseSourceConfig(std.testing.allocator, "kafka_1", &table));
}

test "parse source config rejects type mismatch" {
    const table = TomlTable{
        .entries = &[_]TomlKeyValue{
            .{ .key = "type", .value = TomlValue{ .string = "syslog" } },
            .{ .key = "address", .value = TomlValue{ .integer = 42 } },
        },
    };

    try std.testing.expectError(ParseError.InvalidType, parseSourceConfig(std.testing.allocator, "syslog_bad", &table));
}
