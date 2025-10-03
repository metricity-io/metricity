const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");
const posix = std.posix;

fn closeFd(fd: posix.fd_t) void {
    const info = @typeInfo(@TypeOf(posix.close));
    switch (info) {
        .@"fn" => |fn_info| {
            const ret_type = fn_info.return_type.?;
            switch (@typeInfo(ret_type)) {
                .error_union => posix.close(fd) catch {},
                .void => posix.close(fd),
                else => _ = posix.close(fd),
            }
        },
        else => _ = posix.close(fd),
    }
}

pub fn closeSocket(fd: posix.fd_t) void {
    closeFd(fd);
}

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

fn withSockOpt(comptime field: []const u8) ?i32 {
    if (@hasDecl(posix, "SO")) {
        if (@hasDecl(posix.SO, field)) {
            return @field(posix.SO, field);
        }
    }
    if (@hasDecl(posix, "SO_" ++ field)) {
        return @field(posix, "SO_" ++ field);
    }
    return null;
}

pub fn configureReuseAddr(fd: posix.socket_t) void {
    if (withSockOpt("REUSEADDR")) |opt| {
        var reuse: i32 = 1;
        const opt_u32: u32 = @intCast(opt);
        _ = posix.setsockopt(fd, posix.SOL.SOCKET, opt_u32, std.mem.asBytes(&reuse)) catch {};
    }
}

pub fn configureReuseAddrPort(fd: posix.socket_t) void {
    configureReuseAddr(fd);
    if (withSockOpt("REUSEPORT")) |opt| {
        var reuse: i32 = 1;
        const opt_u32: u32 = @intCast(opt);
        _ = posix.setsockopt(fd, posix.SOL.SOCKET, opt_u32, std.mem.asBytes(&reuse)) catch {};
    }
}

pub fn configureTcpKeepalive(fd: posix.socket_t, seconds: u32) void {
    switch (builtin.os.tag) {
        .linux => {
            var idle: i32 = @intCast(seconds);
            const interval_seconds: u32 = if (seconds >= 3) seconds / 3 else 1;
            var interval: i32 = @intCast(interval_seconds);
            var count: i32 = 3;
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, std.os.linux.TCP.KEEPIDLE, std.mem.asBytes(&idle)) catch {};
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, std.os.linux.TCP.KEEPINTVL, std.mem.asBytes(&interval)) catch {};
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, std.os.linux.TCP.KEEPCNT, std.mem.asBytes(&count)) catch {};
        },
        .macos, .ios, .tvos, .watchos, .visionos, .driverkit => {
            var interval: i32 = @intCast(seconds);
            const DARWIN_TCP_KEEPALIVE: i32 = 0x10;
            _ = posix.setsockopt(fd, posix.IPPROTO.TCP, DARWIN_TCP_KEEPALIVE, std.mem.asBytes(&interval)) catch {};
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
    var writer = stream.writer();
    var adapter = writer.adaptToNewApi(buffer[0..]);
    address.format(&adapter.new_interface) catch {
        return allocator.alloc(u8, 0);
    };
    if (adapter.err) |_| {
        return allocator.alloc(u8, 0);
    }
    return copySlice(allocator, stream.getWritten());
}

pub fn formatPeerAddress(address: std.net.Address, buffer: []u8) []const u8 {
    var stream = std.io.fixedBufferStream(buffer);
    var writer = stream.writer();
    var adapter = writer.adaptToNewApi(buffer);
    address.format(&adapter.new_interface) catch {
        return &[_]u8{};
    };
    if (adapter.err) |_| {
        return &[_]u8{};
    }
    return stream.getWritten();
}

pub fn copySlice(allocator: std.mem.Allocator, source: []const u8) ![]u8 {
    const copy = try allocator.alloc(u8, source.len);
    if (source.len != 0) @memcpy(copy, source);
    return copy;
}
