const std = @import("std");
const xev = @import("xev");

/// Lightweight wrapper around libxev's event loop to centralize setup
/// and give the rest of `netx` a stable surface.
pub const EventLoop = struct {
    loop: xev.Loop,

    pub fn init(options: xev.Options) !EventLoop {
        return EventLoop{ .loop = try xev.Loop.init(options) };
    }

    pub fn initDefault() !EventLoop {
        return init(.{});
    }

    pub fn deinit(self: *EventLoop) void {
        self.loop.deinit();
    }

    pub fn handle(self: *EventLoop) *xev.Loop {
        return &self.loop;
    }

    pub fn run(self: *EventLoop, mode: xev.RunMode) !void {
        try self.loop.run(mode);
    }

    pub fn tick(self: *EventLoop) !void {
        try self.loop.run(.no_wait);
    }
};
