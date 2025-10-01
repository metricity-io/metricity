const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");
const posix = std.posix;

pub fn stripScheme(address: []const u8, scheme: []const u8) []const u8 {
    if (std.mem.startsWith(u8, address, scheme)) {
        return address[scheme.len..];
    }
    return address;
}

pub fn resolveAddress(address_str: []const u8, scheme: []const u8) !std.net.Address {
    const normalized = stripScheme(address_str, scheme);
    const parsed = std.net.Address.parseIpAndPort(normalized) catch return error.InvalidAddress;
    if (parsed.getPort() == 0) return error.InvalidAddress;
    return parsed;
}

pub fn configureReuseAddr(fd: posix.socket_t) void {
    var reuse: i32 = 1;
    _ = posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO_REUSEADDR, std.mem.asBytes(&reuse)) catch {};
}

pub fn configureReuseAddrPort(fd: posix.socket_t) void {
    configureReuseAddr(fd);
    var reuse: i32 = 1;
    _ = posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO_REUSEPORT, std.mem.asBytes(&reuse)) catch {};
}

pub fn configureTcpKeepalive(fd: posix.socket_t, seconds: u32) void {
    switch (builtin.os.tag) {
        .linux, .android => {
            var idle: i32 = @intCast(seconds);
            const interval_seconds: u32 = if (seconds >= 3) seconds / 3 else 1;
            var interval: i32 = @intCast(interval_seconds);
            var count: i32 = 3;
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, std.os.linux.TCP.KEEPIDLE, std.mem.asBytes(&idle)) catch {};
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, std.os.linux.TCP.KEEPINTVL, std.mem.asBytes(&interval)) catch {};
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, std.os.linux.TCP.KEEPCNT, std.mem.asBytes(&count)) catch {};
        },
        .macos, .ios, .tvos, .watchos => {
            var interval: i32 = @intCast(seconds);
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, std.os.darwin.TCP.KEEPALIVE, std.mem.asBytes(&interval)) catch {};
        },
        else => {
            std.log.warn("tcp keepalive not supported on target {s}", .{@tagName(builtin.os.tag)});
        },
    }
}

pub fn fetchPeerAddress(allocator: std.mem.Allocator, socket: xev.TCP) ![]u8 {
    var addr_storage: posix.sockaddr = undefined;
    var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    posix.getpeername(socket.fd, &addr_storage, &addr_len) catch {
        return allocator.alloc(u8, 0);
    };
    const address = std.net.Address.initPosix(@alignCast(&addr_storage));
    var buffer: [128]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buffer);
    address.format(stream.writer()) catch {
        return allocator.alloc(u8, 0);
    };
    return copySlice(allocator, stream.getWritten());
}

pub fn formatPeerAddress(address: std.net.Address, buffer: []u8) []const u8 {
    var stream = std.io.fixedBufferStream(buffer);
    const writer = stream.writer();
    address.format(writer) catch {
        return &[_]u8{};
    };
    return stream.getWritten();
}

pub fn copySlice(allocator: std.mem.Allocator, source: []const u8) ![]u8 {
    const copy = try allocator.alloc(u8, source.len);
    if (source.len != 0) @memcpy(copy, source);
    return copy;
}
