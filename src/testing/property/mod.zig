//! Property-testing helpers and deterministic generators.

const std = @import("std");

pub const Prng = std.Random.DefaultPrng;

pub fn initPrng(seed: u64) Prng {
    return Prng.init(seed);
}

pub fn nextBool(prng: *Prng) bool {
    return prng.random().boolean();
}

pub fn nextRange(prng: *Prng, comptime T: type, min: T, max: T) T {
    if (@TypeOf(min) != T or @TypeOf(max) != T) @compileError("min/max must match type T");
    std.debug.assert(min <= max);
    if (min == max) return min;
    return switch (@typeInfo(T)) {
        .int => |info| switch (info.signedness) {
            .signed => prng.random().intRangeLessThan(T, min, max + 1),
            .unsigned => prng.random().intRangeLessThan(T, min, max + 1),
        },
        .float => prng.random().float(T) * (max - min) + min,
        else => @compileError("Unsupported type for nextRange"),
    };
}
