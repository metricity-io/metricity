//! Shared testing utilities for Metricity.
//! Inspired by TigerBeetle's `src/testing` module layout.

const std = @import("std");

pub const fuzz = @import("fuzz/mod.zig");
pub const property = @import("property/mod.zig");
pub const pipeline = @import("pipeline/mod.zig");
pub const bench = @import("bench/mod.zig");
pub const source = @import("source/mod.zig");

/// Top-level context passed to helpers when additional dependencies are needed.
pub const Context = struct {
    allocator: std.mem.Allocator,
};

/// Placeholder helper; will be extended in follow-up work.
pub fn initContext(allocator: std.mem.Allocator) Context {
    return .{ .allocator = allocator };
}
