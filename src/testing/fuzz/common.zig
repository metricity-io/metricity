//! Common helpers shared across fuzz harnesses.

const std = @import("std");

pub fn randomName(
    prng: *std.Random.DefaultPrng,
    prefix: []const u8,
    buffer: []u8,
    min_tail: usize,
    max_tail: usize,
) []const u8 {
    std.debug.assert(min_tail <= max_tail);
    std.debug.assert(prefix.len + max_tail <= buffer.len);

    var cursor: usize = 0;
    if (prefix.len != 0) {
        @memcpy(buffer[0..prefix.len], prefix);
        cursor = prefix.len;
    }

    const random = prng.random();
    const tail_len = if (max_tail == 0)
        0
    else
        random.intRangeLessThan(usize, min_tail, max_tail + 1);

    var i: usize = 0;
    while (i < tail_len) : (i += 1) {
        const letter = random.intRangeLessThan(u8, 0, 26);
        buffer[cursor] = 'a' + letter;
        cursor += 1;
    }

    return buffer[0..cursor];
}

pub fn randomBytes(
    prng: *std.Random.DefaultPrng,
    allocator: std.mem.Allocator,
    max_len: usize,
) ![]u8 {
    const random = prng.random();
    const len = if (max_len == 0)
        0
    else
        random.intRangeLessThan(usize, 0, max_len + 1);
    const buffer = try allocator.alloc(u8, len);
    random.bytes(buffer);
    return buffer;
}
