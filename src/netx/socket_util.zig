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
    posix.getpeername(socket.fd, &addr_storage, &addr_len) catch |err| {
        std.log.warn("fetchPeerAddress: getpeername failed for fd {d}: {s}", .{ socket.fd, @errorName(err) });
        return allocator.alloc(u8, 0);
    };
    const address = std.net.Address.initPosix(@alignCast(&addr_storage));
    var buffer: [128]u8 = undefined;

    const family = address.any.family;
    const slice = switch (family) {
        posix.AF.INET => blk: {
            const ip = address.in;
            const bytes: *const [4]u8 = @ptrCast(@alignCast(&ip.sa.addr));
            const port = ip.getPort();
            break :blk std.fmt.bufPrint(buffer[0..], "{d}.{d}.{d}.{d}:{d}", .{ bytes[0], bytes[1], bytes[2], bytes[3], port }) catch |err| {
                std.log.warn("fetchPeerAddress: ipv4 format failed for fd {d}: {s}", .{ socket.fd, @errorName(err) });
                return allocator.alloc(u8, 0);
            };
        },
        posix.AF.INET6 => blk: {
            const ip6 = address.in6;
            const parts = [_]u16{
                (@as(u16, ip6.sa.addr[0]) << 8) | @as(u16, ip6.sa.addr[1]),
                (@as(u16, ip6.sa.addr[2]) << 8) | @as(u16, ip6.sa.addr[3]),
                (@as(u16, ip6.sa.addr[4]) << 8) | @as(u16, ip6.sa.addr[5]),
                (@as(u16, ip6.sa.addr[6]) << 8) | @as(u16, ip6.sa.addr[7]),
                (@as(u16, ip6.sa.addr[8]) << 8) | @as(u16, ip6.sa.addr[9]),
                (@as(u16, ip6.sa.addr[10]) << 8) | @as(u16, ip6.sa.addr[11]),
                (@as(u16, ip6.sa.addr[12]) << 8) | @as(u16, ip6.sa.addr[13]),
                (@as(u16, ip6.sa.addr[14]) << 8) | @as(u16, ip6.sa.addr[15]),
            };
            break :blk std.fmt.bufPrint(
                buffer[0..],
                "[{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}]:{d}",
                .{ parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], ip6.getPort() },
            ) catch |err| {
                std.log.warn("fetchPeerAddress: ipv6 format failed for fd {d}: {s}", .{ socket.fd, @errorName(err) });
                return allocator.alloc(u8, 0);
            };
        },
        else => blk: {
            std.log.warn("fetchPeerAddress: unsupported address family {d} for fd {d}", .{ family, socket.fd });
            break :blk buffer[0..0];
        },
    };

    if (slice.len == 0) {
        std.log.warn("fetchPeerAddress: empty formatted address for fd {d} (family {d})", .{ socket.fd, family });
    }
    return copySlice(allocator, slice);
}

pub fn formatPeerAddress(address: std.net.Address, buffer: []u8) []const u8 {
    const family = address.any.family;
    return switch (family) {
        posix.AF.INET => blk: {
            const ip = address.in;
            const bytes: *const [4]u8 = @ptrCast(@alignCast(&ip.sa.addr));
            const port = ip.getPort();
            break :blk std.fmt.bufPrint(buffer, "{d}.{d}.{d}.{d}:{d}", .{ bytes[0], bytes[1], bytes[2], bytes[3], port }) catch {
                return &[_]u8{};
            };
        },
        posix.AF.INET6 => blk: {
            const ip6 = address.in6;
            const parts = [_]u16{
                (@as(u16, ip6.sa.addr[0]) << 8) | @as(u16, ip6.sa.addr[1]),
                (@as(u16, ip6.sa.addr[2]) << 8) | @as(u16, ip6.sa.addr[3]),
                (@as(u16, ip6.sa.addr[4]) << 8) | @as(u16, ip6.sa.addr[5]),
                (@as(u16, ip6.sa.addr[6]) << 8) | @as(u16, ip6.sa.addr[7]),
                (@as(u16, ip6.sa.addr[8]) << 8) | @as(u16, ip6.sa.addr[9]),
                (@as(u16, ip6.sa.addr[10]) << 8) | @as(u16, ip6.sa.addr[11]),
                (@as(u16, ip6.sa.addr[12]) << 8) | @as(u16, ip6.sa.addr[13]),
                (@as(u16, ip6.sa.addr[14]) << 8) | @as(u16, ip6.sa.addr[15]),
            };
            break :blk std.fmt.bufPrint(
                buffer,
                "[{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}]:{d}",
                .{ parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], ip6.getPort() },
            ) catch {
                return &[_]u8{};
            };
        },
        else => buffer[0..0],
    };
}

pub fn copySlice(allocator: std.mem.Allocator, source: []const u8) ![]u8 {
    const copy = try allocator.alloc(u8, source.len);
    if (source.len != 0) @memcpy(copy, source);
    return copy;
}
