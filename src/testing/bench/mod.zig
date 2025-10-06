//! Utilities for load testing and benchmarking.

const std = @import("std");

pub const Sample = struct {
    latency_ns: u64,
};

pub fn record(samples: *std.ArrayList(Sample), allocator: std.mem.Allocator, value: u64) !void {
    try samples.append(allocator, .{ .latency_ns = value });
}
