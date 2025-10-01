const std = @import("std");

pub const Limits = struct {
    read_buffer_bytes: usize = 64 * 1024,
    message_size_limit: usize = 64 * 1024,
};

pub const TcpOptions = struct {
    address: []const u8,
    limits: Limits = .{},
    max_connections: usize = 64,
    keepalive_seconds: ?u32 = null,
    high_watermark: usize = 256 * 1024,
    low_watermark: usize = 128 * 1024,
};

pub const UdpOptions = struct {
    address: []const u8,
    limits: Limits = .{},
};

pub const TransportConfig = union(enum) {
    tcp: TcpOptions,
    udp: UdpOptions,
};

pub fn applyBufferDefaults(limits: Limits) Limits {
    var result = limits;
    if (result.read_buffer_bytes == 0) result.read_buffer_bytes = 1;
    if (result.message_size_limit == 0) result.message_size_limit = 1;
    return result;
}
