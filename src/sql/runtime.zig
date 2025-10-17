const std = @import("std");
const ast = @import("ast.zig");
const source_mod = @import("source");
const plan = @import("plan.zig");
const event_mod = source_mod.event;
const expr_ir = @import("expr_ir.zig");

const ascii = std.ascii;
const math = std.math;
const hash = std.hash;
const hash_map = std.hash_map;
const meta = std.meta;

const IdentifierBinding = struct {
    canonical: []const u8,
    value: []const u8,
    quoted: bool,
};

const TableBinding = struct {
    table: IdentifierBinding,
    alias: ?IdentifierBinding = null,
};

/// Error set representing SQL compilation or evaluation failures triggered by
/// user-provided statements.
pub const Error = std.mem.Allocator.Error || error{
    UnsupportedFeature,
    UnknownColumn,
    TypeMismatch,
    UnsupportedFunction,
    DivideByZero,
    ArithmeticOverflow,
    RowTooLarge,
    GroupStateTooLarge,
    StateBudgetExceeded,
    CpuBudgetExceeded,
    LateEvent,
};

const AggregateFunction = plan.AggregateFunction;
const AggregateArgument = plan.AggregateArgument;
const AggregateSpec = plan.AggregateSpec;
const GroupColumn = plan.GroupColumn;
const Projection = plan.Projection;
const GroupProjection = plan.GroupProjection;
const AggregateProjection = plan.AggregateProjection;

pub const FeatureGate = enum {
    distinct,
    order_by,
    multiple_from,
    join,
    projection_star,
    projection_expression,
    group_by_expression,
    missing_aggregate,
    aggregate_distinct,
    aggregate_argument_arity,
    aggregate_argument_star,
    derived_table,
};

pub const FeatureGateDiagnostic = struct {
    feature: FeatureGate,
    span: ast.Span,
};

threadlocal var last_feature_gate: ?FeatureGateDiagnostic = null;

fn clearFeatureGateDiagnostic() void {
    last_feature_gate = null;
}

fn recordFeatureGate(feature: FeatureGate, span: ast.Span) void {
    last_feature_gate = FeatureGateDiagnostic{
        .feature = feature,
        .span = span,
    };
}

fn failFeature(feature: FeatureGate, span: ast.Span) Error {
    recordFeatureGate(feature, span);
    return Error.UnsupportedFeature;
}

pub fn takeFeatureGateDiagnostic() ?FeatureGateDiagnostic {
    const result = last_feature_gate;
    last_feature_gate = null;
    return result;
}

pub fn featureGateName(feature: FeatureGate) []const u8 {
    return switch (feature) {
        .distinct => "DISTINCT",
        .order_by => "ORDER BY",
        .multiple_from => "multiple FROM tables",
        .join => "JOIN",
        .projection_star => "SELECT *",
        .projection_expression => "non-aggregate projection",
        .group_by_expression => "GROUP BY expression",
        .missing_aggregate => "missing aggregate",
        .aggregate_distinct => "DISTINCT aggregates",
        .aggregate_argument_arity => "aggregate argument count",
        .aggregate_argument_star => "aggregate argument STAR",
        .derived_table => "derived table",
    };
}

const CountState = struct {
    total: u64 = 0,
};

const SumState = struct {
    has_value: bool = false,
    kind: enum { integer, float } = .integer,
    integer_total: i64 = 0,
    float_total: f64 = 0.0,
};

const AvgState = struct {
    sum: SumState = .{},
    count: u64 = 0,
};

const MinMaxState = struct {
    value: event_mod.Value = .{ .null = {} },
    has_value: bool = false,
};

const AggregateState = union(enum) {
    count: CountState,
    sum: SumState,
    avg: AvgState,
    min: MinMaxState,
    max: MinMaxState,

    fn init(function: AggregateFunction) AggregateState {
        return switch (function) {
            .count => .{ .count = .{} },
            .sum => .{ .sum = .{} },
            .avg => .{ .avg = .{} },
            .min => .{ .min = .{} },
            .max => .{ .max = .{} },
        };
    }

    fn deinit(self: *AggregateState, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .min, .max => |*state| {
                if (state.has_value and state.value == .string) {
                    allocator.free(state.value.string);
                    state.value = .{ .null = {} };
                    state.has_value = false;
                }
            },
            else => {},
        }
    }
};

const AggregateJournalEntry = struct {
    index: usize,
    function: AggregateFunction,
    previous: AggregateState,
};

const AggregateJournal = std.ArrayListUnmanaged(AggregateJournalEntry);

const GroupState = struct {
    key_values: []event_mod.Value,
    aggregates: []AggregateState,
    last_seen_ns: u64,
    key_hash: u64,
    size_bytes: usize = 0,

    fn deinit(self: *GroupState, allocator: std.mem.Allocator) void {
        for (self.key_values) |value| {
            freeOwnedValue(allocator, value);
        }
        if (self.key_values.len != 0) allocator.free(self.key_values);

        for (self.aggregates) |*agg| {
            agg.deinit(allocator);
        }
        if (self.aggregates.len != 0) allocator.free(self.aggregates);
    }
};

fn emptyMutableSlice() []u8 {
    // Reuse a single zero-length buffer to avoid unnecessary allocations.
    return @constCast(&[_]u8{});
}

const StringPoolEntry = struct {
    bytes: []u8 = emptyMutableSlice(),
    ref_count: usize = 0,
};

const SerializedGroupKey = struct {
    bytes: []const u8,
    hash: u64,
};

const SerializedGroupKeyContext = struct {
    pub fn hash(self: @This(), key: SerializedGroupKey) u64 {
        _ = self;
        return key.hash;
    }

    pub fn eql(self: @This(), a: SerializedGroupKey, b: SerializedGroupKey) bool {
        _ = self;
        if (a.hash != b.hash) return false;
        return std.mem.eql(u8, a.bytes, b.bytes);
    }
};

const GroupMap = hash_map.HashMapUnmanaged(
    SerializedGroupKey,
    usize,
    SerializedGroupKeyContext,
    hash_map.default_max_load_percentage,
);

const GroupEntry = struct {
    state: GroupState = .{ .key_values = &.{}, .aggregates = &.{}, .last_seen_ns = 0, .key_hash = 0 },
    key_bytes: []u8 = emptyMutableSlice(),
    hash: u64 = 0,
    shard_index: usize = 0,
    lru_prev: ?usize = null,
    lru_next: ?usize = null,
    free_next: ?usize = null,
    occupied: bool = false,
};

const LruList = struct {
    head: ?usize = null,
    tail: ?usize = null,

    fn insert(self: *LruList, entries: []GroupEntry, index: usize) void {
        entries[index].lru_prev = self.tail;
        entries[index].lru_next = null;
        if (self.tail) |tail_index| {
            entries[tail_index].lru_next = index;
        } else {
            self.head = index;
        }
        self.tail = index;
    }

    fn moveToTail(self: *LruList, entries: []GroupEntry, index: usize) void {
        if (self.tail == index) return;
        self.remove(entries, index);
        self.insert(entries, index);
    }

    fn remove(self: *LruList, entries: []GroupEntry, index: usize) void {
        if (entries[index].lru_prev) |prev_index| {
            entries[prev_index].lru_next = entries[index].lru_next;
        } else {
            self.head = entries[index].lru_next;
        }
        if (entries[index].lru_next) |next_index| {
            entries[next_index].lru_prev = entries[index].lru_prev;
        } else {
            self.tail = entries[index].lru_prev;
        }
        entries[index].lru_prev = null;
        entries[index].lru_next = null;
    }
};

const GroupShard = struct {
    map: GroupMap = GroupMap.empty,

    fn deinit(self: *GroupShard, allocator: std.mem.Allocator) void {
        self.map.deinit(allocator);
    }
};

const GroupStore = struct {
    allocator: std.mem.Allocator,
    shards: []GroupShard,
    shard_mask: usize,
    entries: std.ArrayListUnmanaged(GroupEntry) = .{},
    free_head: ?usize = null,
    lru: LruList = .{},
    ttl_ns: ?u64,
    max_groups: usize,
    sweep_interval_ns: u64,
    last_sweep: u64 = 0,
    active_count: usize = 0,
    key_ctx: SerializedGroupKeyContext = .{},
    string_table: std.StringHashMap(u32),
    next_string_id: u32 = 1,
    string_pool: std.ArrayListUnmanaged(StringPoolEntry) = .{},
    max_state_bytes: usize,
    state_bytes: usize = 0,
    pending_ttl_evictions: usize = 0,
    pending_max_groups_evictions: usize = 0,
    pending_state_evictions: usize = 0,

    const Options = struct {
        shard_count: usize = 16,
        ttl_ns: ?u64 = null,
        max_groups: usize = 0,
        sweep_interval_ns: u64 = 0,
        max_state_bytes: usize = 0,
    };

    pub const EvictionReason = enum { ttl, max_groups, state_bytes };

    pub const EvictionStats = struct {
        ttl: usize,
        max_groups: usize,
        state_bytes: usize,

        pub fn isEmpty(self: EvictionStats) bool {
            return self.ttl == 0 and self.max_groups == 0 and self.state_bytes == 0;
        }
    };

    pub const Acquisition = struct {
        index: usize,
        entry: *GroupEntry,
        created: bool,
    };

    pub fn init(allocator: std.mem.Allocator, options: Options) std.mem.Allocator.Error!GroupStore {
        const requested = if (options.shard_count == 0) 1 else options.shard_count;
        const shard_count = normalizeShardCount(requested);
        const shards = try allocator.alloc(GroupShard, shard_count);
        var idx: usize = 0;
        while (idx < shard_count) : (idx += 1) {
            shards[idx] = .{};
        }

        return GroupStore{
            .allocator = allocator,
            .shards = shards,
            .shard_mask = shard_count - 1,
            .ttl_ns = options.ttl_ns,
            .max_groups = options.max_groups,
            .sweep_interval_ns = options.sweep_interval_ns,
            .string_table = std.StringHashMap(u32).init(allocator),
            .max_state_bytes = options.max_state_bytes,
        };
    }

    fn recordEviction(self: *GroupStore, reason: EvictionReason) void {
        switch (reason) {
            .ttl => self.pending_ttl_evictions += 1,
            .max_groups => self.pending_max_groups_evictions += 1,
            .state_bytes => self.pending_state_evictions += 1,
        }
    }

    pub fn takeEvictionStats(self: *GroupStore) ?EvictionStats {
        const stats = EvictionStats{
            .ttl = self.pending_ttl_evictions,
            .max_groups = self.pending_max_groups_evictions,
            .state_bytes = self.pending_state_evictions,
        };
        if (stats.isEmpty()) return null;
        self.pending_ttl_evictions = 0;
        self.pending_max_groups_evictions = 0;
        self.pending_state_evictions = 0;
        return stats;
    }

    pub fn deinit(self: *GroupStore) void {
        for (self.entries.items) |*entry| {
            if (!entry.occupied) continue;
            if (entry.key_bytes.len != 0) {
                self.releaseInternedStrings(entry.key_bytes);
                self.allocator.free(entry.key_bytes);
                entry.key_bytes = emptyMutableSlice();
            }
            entry.state.deinit(self.allocator);
        }
        self.entries.deinit(self.allocator);
        for (self.shards) |*shard| {
            shard.deinit(self.allocator);
        }
        self.string_table.deinit();
        for (self.string_pool.items) |entry| {
            if (entry.bytes.len != 0) self.allocator.free(entry.bytes);
        }
        self.string_pool.deinit(self.allocator);
        self.allocator.free(self.shards);
        self.* = undefined;
    }

    pub fn acquire(
        self: *GroupStore,
        key_bytes: []const u8,
        hash_value: u64,
        now_ns: u64,
    ) (std.mem.Allocator.Error || error{StateBudgetExceeded})!Acquisition {
        self.maybeSweep(now_ns);

        const shard_index = self.shardIndex(hash_value);
        const lookup_key = SerializedGroupKey{ .bytes = key_bytes, .hash = hash_value };
        if (self.shards[shard_index].map.getContext(lookup_key, self.key_ctx)) |existing_index| {
            const idx = existing_index;
            self.touchEntry(idx, now_ns);
            return Acquisition{ .index = idx, .entry = &self.entries.items[idx], .created = false };
        }

        const key_copy = try self.copyKeyBytes(key_bytes);
        const entry_index = try self.allocateEntryIndex();
        const entry = &self.entries.items[entry_index];
        entry.* = GroupEntry{
            .state = .{ .key_values = &.{}, .aggregates = &.{}, .last_seen_ns = now_ns, .key_hash = hash_value },
            .key_bytes = key_copy,
            .hash = hash_value,
            .shard_index = shard_index,
            .lru_prev = null,
            .lru_next = null,
            .free_next = null,
            .occupied = true,
        };

        const shard = &self.shards[entry.shard_index];
        const stored_key = SerializedGroupKey{ .bytes = entry.key_bytes, .hash = entry.hash };
        const gop = try shard.map.getOrPutContext(self.allocator, stored_key, self.key_ctx);
        std.debug.assert(!gop.found_existing);
        gop.value_ptr.* = entry_index;

        self.lru.insert(self.entries.items, entry_index);
        self.active_count += 1;
        _ = self.updateEntryFootprint(entry_index);
        self.enforceLimit();
        if (!self.entries.items[entry_index].occupied) {
            return error.StateBudgetExceeded;
        }

        return Acquisition{ .index = entry_index, .entry = entry, .created = true };
    }

    pub fn getEntry(self: *GroupStore, index: usize) *GroupEntry {
        return &self.entries.items[index];
    }

    pub fn remove(self: *GroupStore, index: usize) void {
        self.removeEntry(index, null);
    }

    pub fn clear(self: *GroupStore) void {
        var idx: usize = 0;
        while (idx < self.entries.items.len) : (idx += 1) {
            if (!self.entries.items[idx].occupied) continue;
            self.removeEntry(idx, null);
        }
        self.entries.clearRetainingCapacity();
        self.free_head = null;
        self.lru = .{};
        self.active_count = 0;
        self.last_sweep = 0;
        self.string_table.deinit();
        self.string_table = std.StringHashMap(u32).init(self.allocator);
        self.next_string_id = 1;
        for (self.string_pool.items) |entry| {
            if (entry.bytes.len != 0) self.allocator.free(entry.bytes);
        }
        self.string_pool.clearRetainingCapacity();
        self.state_bytes = 0;
        self.pending_ttl_evictions = 0;
        self.pending_max_groups_evictions = 0;
        self.pending_state_evictions = 0;
        for (self.shards) |*shard| {
            shard.map.clearRetainingCapacity();
        }
    }

    pub fn count(self: GroupStore) usize {
        return self.active_count;
    }

    pub fn stateBytes(self: *const GroupStore) usize {
        return self.state_bytes;
    }

    fn allocateEntryIndex(self: *GroupStore) std.mem.Allocator.Error!usize {
        if (self.free_head) |reused| {
            self.free_head = self.entries.items[reused].free_next;
            return reused;
        }

        const slot = try self.entries.addOne(self.allocator);
        slot.* = GroupEntry{};
        slot.key_bytes = emptyMutableSlice();
        return self.entries.items.len - 1;
    }

    fn copyKeyBytes(self: *GroupStore, bytes: []const u8) std.mem.Allocator.Error![]u8 {
        if (bytes.len == 0) return emptyMutableSlice();
        const buffer = try self.allocator.alloc(u8, bytes.len);
        std.mem.copyForwards(u8, buffer, bytes);
        return buffer;
    }

    fn internString(self: *GroupStore, value: []const u8) std.mem.Allocator.Error!u32 {
        if (self.string_table.get(value)) |existing_id| {
            const index = @as(usize, existing_id) - 1;
            std.debug.assert(index < self.string_pool.items.len);
            const entry = &self.string_pool.items[index];
            std.debug.assert(entry.ref_count > 0);
            entry.ref_count += 1;
            return existing_id;
        }

        const copy = try self.allocator.alloc(u8, value.len);
        errdefer self.allocator.free(copy);
        if (value.len != 0) {
            std.mem.copyForwards(u8, copy, value);
        }

        const id = self.next_string_id;
        const entry_value = StringPoolEntry{
            .bytes = copy,
            .ref_count = 1,
        };
        try self.string_pool.append(self.allocator, entry_value);
        errdefer {
            // Restore length if insertion into the table fails.
            std.debug.assert(self.string_pool.items.len != 0);
            self.string_pool.items.len -= 1;
        }

        try self.string_table.putNoClobber(copy, id);
        self.next_string_id = id + 1;
        self.state_bytes += copy.len;

        return id;
    }

    fn releaseInternedStringId(self: *GroupStore, id: u32) void {
        if (id == 0) return;
        const index = @as(usize, id) - 1;
        if (index >= self.string_pool.items.len) return;

        const entry = &self.string_pool.items[index];
        std.debug.assert(entry.ref_count > 0);
        entry.ref_count -= 1;
        if (entry.ref_count == 0) {
            if (entry.bytes.len != 0) {
                const removed = self.string_table.remove(entry.bytes);
                std.debug.assert(removed);
                if (self.state_bytes >= entry.bytes.len) {
                    self.state_bytes -= entry.bytes.len;
                } else {
                    self.state_bytes = 0;
                }
                self.allocator.free(entry.bytes);
            }
            entry.bytes = emptyMutableSlice();
        }
    }

    fn releaseInternedStrings(self: *GroupStore, encoded: []const u8) void {
        if (encoded.len == 0) return;

        const Tag = std.meta.Tag(event_mod.Value);
        var offset: usize = 0;
        while (offset < encoded.len) {
            const tag_byte = encoded[offset];
            offset += 1;
            const tag = std.meta.intToEnum(Tag, tag_byte) catch unreachable;
            switch (tag) {
                .string => {
                    std.debug.assert(offset + 4 <= encoded.len);
                    const id_bytes = encoded[offset .. offset + 4];
                    const id_ptr: *const [4]u8 = @ptrCast(id_bytes.ptr);
                    const id = std.mem.readInt(u32, id_ptr, .little);
                    offset += 4;
                    self.releaseInternedStringId(id);
                },
                .integer, .float => {
                    std.debug.assert(offset + 8 <= encoded.len);
                    offset += 8;
                },
                .boolean => {
                    std.debug.assert(offset + 1 <= encoded.len);
                    offset += 1;
                },
                .null => {},
            }
        }
    }

    fn updateEntryFootprint(self: *GroupStore, index: usize) usize {
        const entry = &self.entries.items[index];
        if (!entry.occupied) return 0;
        const old_size = entry.state.size_bytes;
        const new_size = entryFootprint(entry);
        entry.state.size_bytes = new_size;
        if (new_size > old_size) {
            self.state_bytes += new_size - old_size;
        } else {
            const delta = old_size - new_size;
            if (self.state_bytes >= delta) {
                self.state_bytes -= delta;
            } else {
                self.state_bytes = 0;
            }
        }
        return new_size;
    }

    fn touchEntry(self: *GroupStore, index: usize, now_ns: u64) void {
        const entry = &self.entries.items[index];
        entry.state.last_seen_ns = now_ns;
        self.lru.moveToTail(self.entries.items, index);
    }

    fn enforceLimit(self: *GroupStore) void {
        while (self.max_groups != 0 and self.active_count > self.max_groups) {
            const oldest = self.lru.head orelse break;
            self.removeEntry(oldest, .max_groups);
        }

        while (self.max_state_bytes != 0 and self.state_bytes > self.max_state_bytes) {
            const oldest = self.lru.head orelse break;
            self.removeEntry(oldest, .state_bytes);
            if (self.lru.head == null) break;
        }
    }

    pub fn maybeSweep(self: *GroupStore, now_ns: u64) void {
        const interval = self.sweep_interval_ns;
        if (interval == 0) return;
        if (self.last_sweep != 0 and now_ns - self.last_sweep < interval) return;
        self.sweep(now_ns);
    }

    fn sweep(self: *GroupStore, now_ns: u64) void {
        if (self.ttl_ns) |ttl| {
            while (self.lru.head) |index| {
                const entry = &self.entries.items[index];
                if (!entry.occupied) {
                    self.lru.remove(self.entries.items, index);
                    continue;
                }

                const age = if (now_ns > entry.state.last_seen_ns)
                    now_ns - entry.state.last_seen_ns
                else
                    0;
                if (age <= ttl) break;
                self.removeEntry(index, .ttl);
            }
        }

        self.last_sweep = now_ns;
        self.enforceLimit();
    }

    fn removeEntry(self: *GroupStore, index: usize, reason: ?EvictionReason) void {
        const entry = &self.entries.items[index];
        if (!entry.occupied) return;

        self.lru.remove(self.entries.items, index);

        const shard = &self.shards[entry.shard_index];
        const key = SerializedGroupKey{ .bytes = entry.key_bytes, .hash = entry.hash };
        _ = shard.map.fetchRemoveContext(key, self.key_ctx) orelse unreachable;

        if (reason) |r| self.recordEviction(r);

        if (entry.state.size_bytes != 0) {
            if (self.state_bytes >= entry.state.size_bytes) {
                self.state_bytes -= entry.state.size_bytes;
            } else {
                self.state_bytes = 0;
            }
        }

        entry.state.deinit(self.allocator);
        if (entry.key_bytes.len != 0) {
            self.releaseInternedStrings(entry.key_bytes);
            self.allocator.free(entry.key_bytes);
        }

        entry.* = GroupEntry{};
        entry.key_bytes = emptyMutableSlice();
        entry.free_next = self.free_head;
        self.free_head = index;
        self.active_count -= 1;
    }

    fn shardIndex(self: *const GroupStore, hash_value: u64) usize {
        return @intCast(hash_value & @as(u64, self.shard_mask));
    }
};

fn normalizeShardCount(requested: usize) usize {
    if (requested == 0) return 1;
    if (std.math.isPowerOfTwo(requested)) return requested;
    return std.math.ceilPowerOfTwo(usize, requested) catch std.math.maxInt(usize);
}

const GroupAcquisition = struct {
    index: usize,
    entry: *GroupEntry,
    created: bool,
};

fn valueFootprint(value: event_mod.Value) usize {
    return switch (value) {
        .string => |bytes| bytes.len,
        .integer => @sizeOf(i64),
        .float => @sizeOf(f64),
        .boolean => @sizeOf(bool),
        .null => 0,
    };
}

fn aggregateFootprint(state: *const AggregateState) usize {
    return switch (state.*) {
        .count => @sizeOf(CountState),
        .sum => @sizeOf(SumState),
        .avg => @sizeOf(AvgState),
        .min => |min_state| blk: {
            var size: usize = @sizeOf(MinMaxState);
            if (min_state.has_value) {
                size += valueFootprint(min_state.value);
            }
            break :blk size;
        },
        .max => |max_state| blk: {
            var size: usize = @sizeOf(MinMaxState);
            if (max_state.has_value) {
                size += valueFootprint(max_state.value);
            }
            break :blk size;
        },
    };
}

fn groupStateFootprint(state: *const GroupState) usize {
    var total: usize = 0;
    total += state.key_values.len * @sizeOf(event_mod.Value);
    for (state.key_values) |value| {
        total += valueFootprint(value);
    }
    total += state.aggregates.len * @sizeOf(AggregateState);
    for (state.aggregates) |aggregate| {
        total += aggregateFootprint(&aggregate);
    }
    return total;
}

fn entryFootprint(entry: *const GroupEntry) usize {
    var total: usize = entry.key_bytes.len;
    total += groupStateFootprint(&entry.state);
    return total;
}


const ProjectedKeys = struct {
    allocator: std.mem.Allocator,
    values: []event_mod.Value,
    hash: u64,
    allocated: bool,
    encoded: []u8,
    encoded_allocated: bool,
    hash_ready: bool,

    fn empty(allocator: std.mem.Allocator) ProjectedKeys {
        return ProjectedKeys{
            .allocator = allocator,
            .values = &.{},
            .hash = 0,
            .allocated = false,
            .encoded = emptyMutableSlice(),
            .encoded_allocated = false,
            .hash_ready = true,
        };
    }

    fn fromValues(allocator: std.mem.Allocator, values: []event_mod.Value) ProjectedKeys {
        return ProjectedKeys{
            .allocator = allocator,
            .values = values,
            .hash = 0,
            .allocated = values.len != 0,
            .encoded = emptyMutableSlice(),
            .encoded_allocated = false,
            .hash_ready = values.len == 0,
        };
    }

    fn deinit(self: *ProjectedKeys) void {
        if (self.allocated and self.values.len != 0) {
            self.allocator.free(self.values);
        }
        if (self.encoded_allocated and self.encoded.len != 0) {
            self.allocator.free(self.encoded);
        }
        self.values = &.{};
        self.hash = 0;
        self.allocated = false;
        self.encoded = emptyMutableSlice();
        self.encoded_allocated = false;
        self.hash_ready = false;
    }

    fn serialized(self: *ProjectedKeys, store: *GroupStore) std.mem.Allocator.Error![]const u8 {
        if (!self.encoded_allocated and self.values.len != 0) {
            self.encoded = try encodeGroupValues(store, self.allocator, self.values);
            self.encoded_allocated = true;
        }
        if (!self.hash_ready) {
            self.hash = hash.XxHash3.hash(0, self.encoded);
            self.hash_ready = true;
        }
        return self.encoded;
    }
};

const Transaction = struct {
    program: *Program,
    group_index: usize,
    created: bool,
    journal_start: usize,
    active: bool = true,

    fn init(program: *Program, group_index: usize, created: bool) Transaction {
        return Transaction{
            .program = program,
            .group_index = group_index,
            .created = created,
            .journal_start = program.aggregate_journal.items.len,
            .active = true,
        };
    }

    fn commit(self: *Transaction, now_ns: u64) Error!void {
        if (!self.active) return;
        errdefer self.rollback();

        const entry = self.program.group_store.getEntry(self.group_index);
        entry.state.last_seen_ns = now_ns;

        try self.program.finalizeGroup(self.group_index, now_ns);

        const journal_slice = self.program.aggregate_journal.items[self.journal_start..self.program.aggregate_journal.items.len];
        for (journal_slice) |journal_record| {
            switch (journal_record.function) {
                .min => {
                    const snapshot = journal_record.previous.min;
                    if (snapshot.has_value) {
                        freeOwnedValue(self.program.allocator, snapshot.value);
                    }
                },
                .max => {
                    const snapshot = journal_record.previous.max;
                    if (snapshot.has_value) {
                        freeOwnedValue(self.program.allocator, snapshot.value);
                    }
                },
                else => {},
            }
        }

        self.program.aggregate_journal.items.len = self.journal_start;
        self.active = false;
    }

    fn rollback(self: *Transaction) void {
        if (!self.active) return;

        const journal = self.program.aggregate_journal.items[self.journal_start..self.program.aggregate_journal.items.len];
        if (self.created) {
            self.program.group_store.remove(self.group_index);
        } else {
            var idx: usize = journal.len;
            const entry = self.program.group_store.getEntry(self.group_index);
            while (idx > 0) {
                idx -= 1;
                const journal_entry = journal[idx];
                const state = &entry.state.aggregates[journal_entry.index];
                switch (journal_entry.function) {
                    .count => state.count = journal_entry.previous.count,
                    .sum => state.sum = journal_entry.previous.sum,
                    .avg => state.avg = journal_entry.previous.avg,
                    .min => {
                        if (state.min.has_value) {
                            freeOwnedValue(self.program.allocator, state.min.value);
                        }
                        state.min = journal_entry.previous.min;
                    },
                    .max => {
                        if (state.max.has_value) {
                            freeOwnedValue(self.program.allocator, state.max.value);
                        }
                        state.max = journal_entry.previous.max;
                    },
                }
            }
            _ = self.program.group_store.updateEntryFootprint(self.group_index);
        }

        self.program.aggregate_journal.items.len = self.journal_start;
        self.active = false;
    }

    fn release(self: *Transaction) void {
        if (!self.active) return;
        self.rollback();
    }

    fn record(self: *Transaction, function: AggregateFunction, index: usize, state: *const AggregateState) void {
        if (!self.active) return;
        self.program.aggregate_journal.appendAssumeCapacity(.{
            .index = index,
            .function = function,
            .previous = state.*,
        });
    }
};

pub const EvictionConfig = struct {
    ttl_ns: ?u64 = 15 * std.time.ns_per_min,
    max_groups: usize = 50_000,
    sweep_interval_ns: u64 = 60 * std.time.ns_per_s,
};

pub const LimitConfig = struct {
    max_state_bytes: usize = 0,
    max_row_bytes: usize = 0,
    max_group_bytes: usize = 0,
    cpu_budget_ns_per_second: u64 = 0,
    late_event_threshold_ns: ?u64 = null,
};

pub const ErrorPolicy = enum {
    skip_event,
    null,
    clamp,
    propagate,
};

const CpuBudgetTracker = struct {
    budget_ns: u64,
    window_start_ns: i128 = 0,
    used_ns: u64 = 0,

    fn init(budget_ns: u64) CpuBudgetTracker {
        return .{ .budget_ns = budget_ns };
    }

    fn consume(self: *CpuBudgetTracker, now_ns: i128, cost_ns: u64) bool {
        if (self.budget_ns == 0) return true;
        if (self.window_start_ns == 0 or now_ns - self.window_start_ns >= @as(i128, @intCast(std.time.ns_per_s))) {
            self.window_start_ns = now_ns;
            self.used_ns = 0;
        }

        if (self.used_ns >= self.budget_ns) {
            return false;
        }

        const remaining = self.budget_ns - self.used_ns;
        if (cost_ns > remaining) {
            self.used_ns = self.budget_ns;
            return false;
        }

        self.used_ns += cost_ns;
        return true;
    }
};

pub const CompileOptions = struct {
    eviction: EvictionConfig = .{},
    limits: LimitConfig = .{},
    error_policy: ErrorPolicy = .skip_event,
};

/// Compiled representation of a SQL `SELECT` statement that can be executed
/// against pipeline events.
pub const Program = struct {
    allocator: std.mem.Allocator,
    plan: plan.PhysicalPlan,
    table_binding: ?TableBinding,
    group_store: GroupStore,
    eviction: EvictionConfig,
    limits: LimitConfig,
    error_policy: ErrorPolicy,
    cpu_tracker: CpuBudgetTracker,
    row_adjustment: RowAdjustment = .none,
    filter_program: ?expr_ir.Program = null,
    having_program: ?expr_ir.Program = null,
    aggregate_argument_programs: []?expr_ir.Program = &.{},
    group_key_bindings: []expr_ir.EventBinding = &.{},
    aggregate_journal: AggregateJournal = .{},

    /// Releases memory allocated for the program metadata and accumulated state.
    pub fn deinit(self: *Program) void {
        self.group_store.deinit();

        if (self.filter_program) |program| {
            program.deinit(self.allocator);
        }
        if (self.having_program) |program| {
            program.deinit(self.allocator);
        }
        for (self.aggregate_argument_programs) |maybe_program| {
            if (maybe_program) |program| {
                program.deinit(self.allocator);
            }
        }
        if (self.aggregate_argument_programs.len != 0) {
            self.allocator.free(self.aggregate_argument_programs);
        }
        if (self.group_key_bindings.len != 0) {
            self.allocator.free(self.group_key_bindings);
        }

        if (self.aggregate_journal.capacity != 0) {
            self.aggregate_journal.deinit(self.allocator);
        }

        self.allocator.free(self.plan.project.projections);
        self.allocator.free(self.plan.project.group_columns);
        self.allocator.free(self.plan.group_aggregate.aggregates);
    }

    /// Clears accumulated aggregation state, preserving compiled metadata.
    pub fn reset(self: *Program) void {
        self.group_store.clear();
        self.aggregate_journal.items.len = 0;
        self.cpu_tracker.window_start_ns = 0;
        self.cpu_tracker.used_ns = 0;
        self.row_adjustment = .none;
    }

    pub fn consumeCpuBudget(self: *Program, now_ns: i128, cost_ns: u64) bool {
        return self.cpu_tracker.consume(now_ns, cost_ns);
    }

    pub fn lateEventThreshold(self: *const Program) ?u64 {
        return self.limits.late_event_threshold_ns;
    }

    pub fn takeEvictions(self: *Program) ?GroupStore.EvictionStats {
        return self.group_store.takeEvictionStats();
    }

    pub fn stateBytes(self: *const Program) usize {
        return self.group_store.stateBytes();
    }

    pub fn groupCount(self: *const Program) usize {
        return self.group_store.count();
    }

    pub fn takeRowAdjustment(self: *Program) RowAdjustment {
        const result = self.row_adjustment;
        self.row_adjustment = .none;
        return result;
    }

    fn finalizeGroup(self: *Program, index: usize, now_ns: u64) Error!void {
        self.group_store.touchEntry(index, now_ns);
        const new_size = self.group_store.updateEntryFootprint(index);
        if (self.limits.max_group_bytes != 0 and new_size > self.limits.max_group_bytes) {
            return Error.GroupStateTooLarge;
        }
        if (self.limits.max_state_bytes != 0 and new_size > self.limits.max_state_bytes) {
            return Error.StateBudgetExceeded;
        }

        self.group_store.enforceLimit();
        if (self.limits.max_state_bytes != 0 and self.group_store.stateBytes() > self.limits.max_state_bytes) {
            return Error.StateBudgetExceeded;
        }
    }

    /// Evaluates the program against a single event, returning an optional row.
    /// When the `WHERE` or `HAVING` clause filters out the event, `null` is returned.
    pub fn execute(
        self: *Program,
        allocator: std.mem.Allocator,
        event: *const event_mod.Event,
        now_ns: u64,
    ) Error!?Row {
        const exec_start = std.time.nanoTimestamp();
        return self.executeTracked(allocator, event, now_ns, exec_start, null);
    }

    /// Executes the program while reporting the observed CPU duration. When `duration_out`
    /// is provided the caller receives the elapsed nanoseconds spent inside this method.
    pub fn executeTracked(
        self: *Program,
        allocator: std.mem.Allocator,
        event: *const event_mod.Event,
        now_ns: u64,
        exec_start_ns: i128,
        duration_out: ?*u64,
    ) Error!?Row {
        return self.executeInternal(allocator, event, now_ns, exec_start_ns, duration_out);
    }

    fn executeInternal(
        self: *Program,
        allocator: std.mem.Allocator,
        event: *const event_mod.Event,
        now_ns: u64,
        exec_start_ns: i128,
        duration_out: ?*u64,
    ) Error!?Row {
        if (duration_out) |ptr| ptr.* = 0;
        self.group_store.maybeSweep(now_ns);

        var event_accessor_ctx = EventAccessorContext{ .event = event };

        if (self.filter_program) |*program| {
            const exec_ctx = expr_ir.ExecutionContext{
                .event_accessor = expr_ir.EventAccessor{
                    .context = @ptrCast(@constCast(&event_accessor_ctx)),
                    .load_fn = loadEventValue,
                },
            };
            const predicate = try self.executePredicateProgram(program, exec_ctx, false);
            if (!predicate) return null;
        }

        var projected_keys = try self.runProjectStage(allocator, event);
        defer projected_keys.deinit();

        const acquisition = try self.runGroupAggregateStage(&projected_keys, now_ns);
        var entry = acquisition.entry;
        var group_ptr = &entry.state;

        var txn = Transaction.init(self, acquisition.index, acquisition.created);
        defer txn.release();

        try self.updateAggregates(&txn, group_ptr, event);

        if (self.having_program) |*program| {
            var aggregate_ctx = AggregateAccessorContext{
                .program = self,
                .group = group_ptr,
            };
            const exec_ctx = expr_ir.ExecutionContext{
                .group_values = group_ptr.key_values,
                .aggregate_accessor = expr_ir.AggregateAccessor{
                    .context = @ptrCast(@constCast(&aggregate_ctx)),
                    .load_fn = loadAggregateValue,
                },
            };
            const keep = try self.executePredicateProgram(program, exec_ctx, true);
            if (!keep) {
                const exec_end = std.time.nanoTimestamp();
                var duration_ns: u64 = 0;
                const duration_i128 = exec_end - exec_start_ns;
                if (duration_i128 > 0) {
                    duration_ns = @as(u64, @intCast(duration_i128));
                }
                if (duration_out) |ptr| ptr.* = duration_ns;
                if (duration_ns != 0 and !self.consumeCpuBudget(exec_end, duration_ns)) {
                    return Error.CpuBudgetExceeded;
                }
                try txn.commit(now_ns);
                return null;
            }
        }

        const project_stage = self.plan.project;
        const aggregate_stage = self.plan.group_aggregate;
        const total_columns = project_stage.projections.len;
        var values = try allocator.alloc(ValueEntry, total_columns);
        var idx: usize = 0;
        errdefer allocator.free(values);

        for (project_stage.projections) |proj| {
            switch (proj) {
                .group => |group_proj| {
                    values[idx] = .{
                        .name = group_proj.label,
                        .value = try cloneRowValue(allocator, group_ptr.key_values[group_proj.column_index]),
                    };
                    idx += 1;
                },
                .aggregate => |agg_proj| {
                    const spec = &aggregate_stage.aggregates[agg_proj.aggregate_index];
                    const value = try aggregateResultValue(&group_ptr.aggregates[agg_proj.aggregate_index], spec);
                    values[idx] = .{
                        .name = agg_proj.label,
                        .value = try cloneRowValue(allocator, value),
                    };
                    idx += 1;
                },
            }
        }

        std.debug.assert(idx == total_columns);
        var row = Row{ .allocator = allocator, .values = values, .owns_strings = true };
        var drop_row = false;

        self.row_adjustment = .none;
        if (self.limits.max_row_bytes != 0) {
            var row_bytes = row.footprint();
            if (row_bytes > self.limits.max_row_bytes) {
                switch (self.error_policy) {
                    .clamp => {
                        if (!try row.clampToBytes(self.limits.max_row_bytes)) {
                            row.deinit();
                            return Error.RowTooLarge;
                        }
                        row_bytes = row.footprint();
                        if (row_bytes > self.limits.max_row_bytes) {
                            row.deinit();
                            return Error.RowTooLarge;
                        }
                        self.row_adjustment = .clamped;
                    },
                    .null => {
                        row.deinit();
                        self.row_adjustment = .nullified;
                        drop_row = true;
                    },
                    .skip_event, .propagate => {
                        row.deinit();
                        return Error.RowTooLarge;
                    },
                }
            }
        }

        if (drop_row) {
            const exec_end = std.time.nanoTimestamp();
            var duration_ns: u64 = 0;
            const duration_i128 = exec_end - exec_start_ns;
            if (duration_i128 > 0) {
                duration_ns = @as(u64, @intCast(duration_i128));
            }
            if (duration_out) |ptr| ptr.* = duration_ns;
            if (duration_ns != 0 and !self.consumeCpuBudget(exec_end, duration_ns)) {
                return Error.CpuBudgetExceeded;
            }

            try txn.commit(now_ns);
            return null;
        }

        if (self.plan.window) |window_stage| {
            try self.runWindowStage(window_stage, &row, now_ns);
        }

        if (self.plan.route) |route_stage| {
            try self.runRouteStage(route_stage, &row);
        }

        const exec_end = std.time.nanoTimestamp();
        var duration_ns: u64 = 0;
        const duration_i128 = exec_end - exec_start_ns;
        if (duration_i128 > 0) {
            duration_ns = @as(u64, @intCast(duration_i128));
        }
        if (duration_out) |ptr| ptr.* = duration_ns;
        if (duration_ns != 0 and !self.consumeCpuBudget(exec_end, duration_ns)) {
            return Error.CpuBudgetExceeded;
        }

        try txn.commit(now_ns);
        entry = self.group_store.getEntry(acquisition.index);
        group_ptr = &entry.state;
        group_ptr.key_hash = projected_keys.hash;

        return row;
    }

    fn runProjectStage(self: *Program, allocator: std.mem.Allocator, event: *const event_mod.Event) Error!ProjectedKeys {
        const group_columns = self.plan.project.group_columns;
        if (group_columns.len == 0) {
            return ProjectedKeys.empty(allocator);
        }

        var values = try allocator.alloc(event_mod.Value, group_columns.len);
        var ok = false;
        errdefer if (!ok) allocator.free(values);

        var event_ctx = EventAccessorContext{ .event = event };
        const context_ptr = @as(*anyopaque, @ptrCast(@constCast(&event_ctx)));
        for (self.group_key_bindings, 0..) |binding_desc, idx| {
            values[idx] = try loadEventValue(context_ptr, binding_desc);
        }

        ok = true;
        return ProjectedKeys.fromValues(allocator, values);
    }

    fn runGroupAggregateStage(self: *Program, projected: *ProjectedKeys, now_ns: u64) Error!GroupAcquisition {
        const serialized = try projected.serialized(&self.group_store);
        const acquisition = try self.group_store.acquire(serialized, projected.hash, now_ns);

        if (acquisition.created) {
            errdefer self.group_store.remove(acquisition.index);
            try self.initializeGroupEntry(acquisition.entry, projected.values, projected.hash, now_ns);
        } else {
            self.group_store.releaseInternedStrings(serialized);
        }

        return GroupAcquisition{
            .index = acquisition.index,
            .entry = acquisition.entry,
            .created = acquisition.created,
        };
    }

    fn runWindowStage(self: *Program, stage: plan.WindowStage, row: *Row, now_ns: u64) Error!void {
        _ = self;
        _ = now_ns;
        if (!stage.enabled) return;
        _ = row;
    }

    fn runRouteStage(self: *Program, stage: plan.RouteStage, row: *Row) Error!void {
        _ = self;
        _ = stage;
        _ = row;
    }
    fn copyKeyValues(self: *Program, values: []const event_mod.Value) Error![]event_mod.Value {
        if (values.len == 0) return &.{};
        var output = try self.allocator.alloc(event_mod.Value, values.len);
        var idx: usize = 0;
        errdefer {
            while (idx > 0) {
                idx -= 1;
                freeOwnedValue(self.allocator, output[idx]);
            }
            self.allocator.free(output);
        }
        for (values, 0..) |value, i| {
            output[i] = try copyValueOwned(self.allocator, value);
            idx += 1;
        }
        return output;
    }

    fn createAggregateStateSlice(self: *Program) Error![]AggregateState {
        const aggregates = self.plan.group_aggregate.aggregates;
        if (aggregates.len == 0) return &.{};
        var states = try self.allocator.alloc(AggregateState, aggregates.len);
        var idx: usize = 0;
        errdefer {
            while (idx > 0) {
                idx -= 1;
                states[idx].deinit(self.allocator);
            }
            self.allocator.free(states);
        }
        for (aggregates, 0..) |spec, i| {
            states[i] = AggregateState.init(spec.function);
            idx += 1;
        }
        return states;
    }

    fn initializeGroupEntry(
        self: *Program,
        entry: *GroupEntry,
        values: []const event_mod.Value,
        hash_value: u64,
        now_ns: u64,
    ) Error!void {
        const copied = try self.copyKeyValues(values);
        errdefer {
            if (copied.len != 0) {
                var i = copied.len;
                while (i > 0) {
                    i -= 1;
                    freeOwnedValue(self.allocator, copied[i]);
                }
                self.allocator.free(copied);
            }
        }

        const aggregates = try self.createAggregateStateSlice();
        errdefer self.destroyAggregateStateSlice(aggregates);

        entry.state.key_values = copied;
        entry.state.aggregates = aggregates;
        entry.state.last_seen_ns = now_ns;
        entry.state.key_hash = hash_value;
    }

    fn updateAggregates(self: *Program, txn: *Transaction, group: *GroupState, event: *const event_mod.Event) Error!void {
        for (self.plan.group_aggregate.aggregates, 0..) |spec, idx| {
            const input = try self.evaluateAggregateArgument(idx, spec, event);
            const state = &group.aggregates[idx];
            switch (spec.function) {
                .count => try updateCountState(txn, idx, state, spec, input),
                .sum => try updateSumState(txn, idx, state, spec, input),
                .avg => try updateAvgState(txn, idx, state, input),
                .min => try updateMinMaxState(txn, idx, state, self.allocator, input, .min),
                .max => try updateMinMaxState(txn, idx, state, self.allocator, input, .max),
            }
        }
    }

    fn executePredicateProgram(
        self: *Program,
        program: *const expr_ir.Program,
        exec_context: expr_ir.ExecutionContext,
        treat_null_as_false: bool,
    ) Error!bool {
        _ = self;
        const value = try expr_ir.execute(program, exec_context);
        return switch (value) {
            .boolean => |b| b,
            .null => if (treat_null_as_false) false else Error.TypeMismatch,
            else => Error.TypeMismatch,
        };
    }

    fn evaluateAggregateArgument(
        self: *Program,
        index: usize,
        spec: AggregateSpec,
        event: *const event_mod.Event,
    ) Error!AggregateInput {
        return switch (spec.argument) {
            .star => AggregateInput.count_all,
            .expression => {
                if (index >= self.aggregate_argument_programs.len) return Error.UnsupportedFeature;
                if (self.aggregate_argument_programs[index]) |*program| {
                    var event_ctx = EventAccessorContext{ .event = event };
                    const exec_ctx = expr_ir.ExecutionContext{
                        .event_accessor = expr_ir.EventAccessor{
                            .context = @ptrCast(@constCast(&event_ctx)),
                            .load_fn = loadEventValue,
                        },
                    };
                    const value = try expr_ir.execute(program, exec_ctx);
                    if (value == .null and spec.function != .count) {
                        return AggregateInput.skip;
                    }
                    if (spec.function == .count and value == .null) {
                        return AggregateInput.skip;
                    }
                    return AggregateInput{ .value = value };
                }
                return Error.UnsupportedFeature;
            },
        };
    }

    fn destroyAggregateStateSlice(self: *Program, states: []AggregateState) void {
        if (states.len == 0) return;
        for (states) |*agg| {
            agg.deinit(self.allocator);
        }
        self.allocator.free(states);
    }
};

const RowAdjustment = enum { none, clamped, nullified };

/// Result row consumed by sink implementations.
pub const Row = struct {
    allocator: std.mem.Allocator,
    values: []ValueEntry,
    owns_strings: bool = false,

    pub fn footprint(self: *const Row) usize {
        var total: usize = 0;
        for (self.values) |entry| {
            total += entry.name.len;
            total += valueFootprint(entry.value);
        }
        return total;
    }

    pub fn clampToBytes(self: *Row, limit: usize) !bool {
        var current = self.footprint();
        if (current <= limit) return true;

        if (self.values.len == 0) return false;

        var idx: usize = 0;
        while (idx < self.values.len and current > limit) : (idx += 1) {
            var entry = &self.values[idx];
            if (entry.value != .string) continue;
            const slice = entry.value.string;
            if (slice.len == 0) continue;
            const excess = current - limit;
            const reduce = if (excess < slice.len) excess else slice.len;
            const new_len = slice.len - reduce;
            if (self.owns_strings) {
                const trimmed = try self.allocator.dupe(u8, slice[0..new_len]);
                self.allocator.free(slice);
                entry.value = .{ .string = trimmed };
            } else {
                entry.value = .{ .string = slice[0..new_len] };
            }
            current = self.footprint();
        }

        return current <= limit;
    }

    pub fn setAllNull(self: *Row) void {
        for (self.values) |*entry| {
            if (self.owns_strings and entry.value == .string) {
                self.allocator.free(entry.value.string);
            }
            entry.value = .{ .null = {} };
        }
    }

    /// Releases ownership of value entries.
    pub fn deinit(self: Row) void {
        if (self.owns_strings) {
            for (self.values) |entry| {
                if (entry.value == .string) {
                    self.allocator.free(entry.value.string);
                }
            }
        }
        self.allocator.free(self.values);
    }
};

test "row clamp shrinks string payload" {
    const allocator = std.testing.allocator;

    var entries = try allocator.alloc(ValueEntry, 1);

    const original = try allocator.dupe(u8, "abcdefghij");
    entries[0] = .{ .name = "col", .value = .{ .string = original } };

    var row = Row{ .allocator = allocator, .values = entries, .owns_strings = true };
    defer row.deinit();

    try std.testing.expectEqual(@as(usize, 13), row.footprint());
    try std.testing.expect(try row.clampToBytes(8));
    try std.testing.expectEqual(@as(usize, 8), row.footprint());
    try std.testing.expectEqual(@as(usize, 5), row.values[0].value.string.len);
}

test "row setAllNull releases owned strings" {
    const allocator = std.testing.allocator;

    var entries = try allocator.alloc(ValueEntry, 1);

    const original = try allocator.dupe(u8, "payload");
    entries[0] = .{ .name = "col", .value = .{ .string = original } };

    var row = Row{ .allocator = allocator, .values = entries, .owns_strings = true };

    row.setAllNull();
    try std.testing.expect(row.values[0].value == .null);

    row.deinit();
}

/// Named value produced by SQL execution.
pub const ValueEntry = struct {
    name: []const u8,
    value: event_mod.Value,
};

pub fn compile(allocator: std.mem.Allocator, stmt: *const ast.SelectStatement, options: CompileOptions) Error!Program {
    clearFeatureGateDiagnostic();

    if (stmt.distinct) {
        const span = stmt.distinct_span orelse stmt.span;
        return failFeature(.distinct, span);
    }
    if (stmt.order_by.len != 0) {
        const span = stmt.order_by_span orelse stmt.order_by[0].span;
        return failFeature(.order_by, span);
    }
    if (stmt.from.len > 1) {
        const span = stmt.from[1].span;
        return failFeature(.multiple_from, span);
    }
    for (stmt.from) |table_expr| {
        if (table_expr.joins.len != 0) {
            const span = table_expr.joins[0].span;
            return failFeature(.join, span);
        }
    }

    var group_columns_builder = std.ArrayListUnmanaged(GroupColumn){};
    defer group_columns_builder.deinit(allocator);

    const table_binding = try determineTableBinding(stmt);

    for (stmt.group_by) |expr| {
        switch (expr.*) {
            .column => |col| {
                if (col.table) |qualifier| {
                    if (!qualifierAllowed(table_binding, qualifier)) return Error.UnknownColumn;
                }
                try group_columns_builder.append(allocator, .{ .column = col });
            },
            else => return failFeature(.group_by_expression, ast.expressionSpan(expr)),
        }
    }

    var aggregates_builder = std.ArrayListUnmanaged(AggregateSpec){};
    defer aggregates_builder.deinit(allocator);

    var projection_builder = std.ArrayListUnmanaged(Projection){};
    defer projection_builder.deinit(allocator);

    for (stmt.projection) |item| {
        switch (item.kind) {
            .star => return failFeature(.projection_star, item.span),
            .expression => |expr| {
                if (classifyAggregate(expr)) |call_info| {
                    const index = try appendAggregateSpec(allocator, &aggregates_builder, call_info.call);
                    const label = if (item.alias) |alias_ident| alias_ident.text() else call_info.default_label;
                    try projection_builder.append(allocator, .{
                        .aggregate = .{
                            .label = label,
                            .aggregate_index = index,
                        },
                    });
                } else if (expr.* == .column) {
                    const column = expr.column;
                    const index = findGroupColumnIndex(group_columns_builder.items, column, table_binding) orelse {
                        if (column.table) |qualifier| {
                            if (!qualifierAllowed(table_binding, qualifier)) return Error.UnknownColumn;
                        }
                        return failFeature(.projection_expression, ast.expressionSpan(expr));
                    };
                    const label = if (item.alias) |alias_ident| alias_ident.text() else column.name.text();
                    try projection_builder.append(allocator, .{
                        .group = .{
                            .label = label,
                            .column_index = index,
                        },
                    });
                } else {
                    return failFeature(.projection_expression, ast.expressionSpan(expr));
                }
            },
        }
    }

    if (aggregates_builder.items.len == 0) {
        return failFeature(.missing_aggregate, stmt.span); // stateful engine requires at least one aggregate
    }

    if (stmt.having) |expr| {
        try registerAggregatesInExpression(allocator, expr, &aggregates_builder);
    }

    const group_columns = try group_columns_builder.toOwnedSlice(allocator);
    errdefer allocator.free(group_columns);

    const aggregates = try aggregates_builder.toOwnedSlice(allocator);
    errdefer allocator.free(aggregates);

    const projections = try projection_builder.toOwnedSlice(allocator);
    errdefer allocator.free(projections);

    const filter_stage = if (stmt.selection) |expr|
        plan.FilterStage{ .predicate = expr }
    else
        null;

    const having_stage = if (stmt.having) |expr|
        plan.HavingStage{ .predicate = expr }
    else
        null;

    const project_stage = plan.ProjectStage{
        .projections = projections,
        .group_columns = group_columns,
    };

    const aggregate_stage = plan.GroupAggregateStage{
        .aggregates = aggregates,
    };

    var filter_program_opt: ?expr_ir.Program = null;
    var having_program_opt: ?expr_ir.Program = null;
    var aggregate_argument_programs: []?expr_ir.Program = &.{};
    var group_key_bindings: []expr_ir.EventBinding = &.{};
    errdefer {
        if (filter_program_opt) |program| program.deinit(allocator);
        if (having_program_opt) |program| program.deinit(allocator);
        if (aggregate_argument_programs.len != 0) {
            for (aggregate_argument_programs) |maybe_program| {
                if (maybe_program) |program| program.deinit(allocator);
            }
            allocator.free(aggregate_argument_programs);
        }
        if (group_key_bindings.len != 0) {
            allocator.free(group_key_bindings);
        }
    }

    if (group_columns.len != 0) {
        group_key_bindings = try allocator.alloc(expr_ir.EventBinding, group_columns.len);
        var event_ctx = EventResolverContext{ .table_binding = table_binding };
        const context_ptr = @as(*anyopaque, @ptrCast(@constCast(&event_ctx)));
        for (group_columns, 0..) |group_col, idx| {
            group_key_bindings[idx] = try resolveEventBinding(context_ptr, group_col.column);
        }
    }

    if (filter_stage) |stage| {
        var event_ctx = EventResolverContext{ .table_binding = table_binding };
        const env = expr_ir.Environment{
            .event_resolver = expr_ir.EventResolver{
                .context = @ptrCast(@constCast(&event_ctx)),
                .resolve_fn = resolveEventBinding,
            },
        };
        const filter_options = expr_ir.CompileOptions{
            .allow_event_columns = true,
            .expect_boolean_result = true,
        };
        filter_program_opt = try expr_ir.compileExpression(allocator, stage.predicate, filter_options, env);
    }

    if (having_stage) |stage| {
        var group_ctx = GroupResolverContext{
            .columns = group_columns,
            .table_binding = table_binding,
        };
        var aggregate_ctx = AggregateResolverContext{
            .specs = aggregates,
        };
        const env = expr_ir.Environment{
            .group_resolver = expr_ir.GroupResolver{
                .context = @ptrCast(@constCast(&group_ctx)),
                .resolve_fn = resolveGroupBinding,
            },
            .aggregate_resolver = expr_ir.AggregateResolver{
                .context = @ptrCast(@constCast(&aggregate_ctx)),
                .resolve_fn = resolveAggregateBinding,
            },
        };
        const having_options = expr_ir.CompileOptions{
            .allow_group_columns = true,
            .allow_aggregate_refs = true,
            .expect_boolean_result = true,
        };
        having_program_opt = try expr_ir.compileExpression(allocator, stage.predicate, having_options, env);
    }

    if (aggregates.len != 0) {
        aggregate_argument_programs = try allocator.alloc(?expr_ir.Program, aggregates.len);
        for (aggregate_argument_programs, 0..) |*slot, idx| {
            slot.* = null;
            const spec = aggregates[idx];
            if (spec.argument == .expression) {
                var event_ctx = EventResolverContext{ .table_binding = table_binding };
                const env = expr_ir.Environment{
                    .event_resolver = expr_ir.EventResolver{
                        .context = @ptrCast(@constCast(&event_ctx)),
                        .resolve_fn = resolveEventBinding,
                    },
                };
                const argument_options = expr_ir.CompileOptions{
                    .allow_event_columns = true,
                };
                slot.* = try expr_ir.compileExpression(allocator, spec.argument.expression, argument_options, env);
            }
        }
    }

    const physical_plan = plan.PhysicalPlan{
        .filter = filter_stage,
        .project = project_stage,
        .group_aggregate = aggregate_stage,
        .having = having_stage,
        .window = null,
        .route = null,
    };

    var store = try GroupStore.init(allocator, .{
        .ttl_ns = options.eviction.ttl_ns,
        .max_groups = options.eviction.max_groups,
        .sweep_interval_ns = options.eviction.sweep_interval_ns,
        .max_state_bytes = options.limits.max_state_bytes,
    });
    errdefer store.deinit();

    var program = Program{
        .allocator = allocator,
        .plan = physical_plan,
        .table_binding = table_binding,
        .group_store = store,
        .eviction = options.eviction,
        .limits = options.limits,
        .error_policy = options.error_policy,
        .cpu_tracker = CpuBudgetTracker.init(options.limits.cpu_budget_ns_per_second),
        .filter_program = filter_program_opt,
        .having_program = having_program_opt,
        .aggregate_argument_programs = aggregate_argument_programs,
        .group_key_bindings = group_key_bindings,
    };

    if (aggregate_stage.aggregates.len != 0) {
        try program.aggregate_journal.ensureTotalCapacity(allocator, aggregate_stage.aggregates.len);
    }

    return program;
}

const AggregateCallInfo = struct {
    call: *const ast.FunctionCall,
    default_label: []const u8,
};

fn classifyAggregate(expr: *const ast.Expression) ?AggregateCallInfo {
    switch (expr.*) {
        .function_call => |*call| {
            _ = aggregateFunctionFromName(call.name.canonical) orelse return null;
            return AggregateCallInfo{ .call = call, .default_label = call.name.text() };
        },
        else => return null,
    }
}

fn appendAggregateSpec(
    allocator: std.mem.Allocator,
    builder: *std.ArrayListUnmanaged(AggregateSpec),
    call: *const ast.FunctionCall,
) Error!usize {
    if (call.distinct) return failFeature(.aggregate_distinct, call.span);

    const function = aggregateFunctionFromName(call.name.canonical) orelse return Error.UnsupportedFunction;
    const argument = switch (function) {
        .count => try parseCountArgument(call),
        else => try parseSingleArgument(call),
    };

    for (builder.items, 0..) |spec, idx| {
        if (spec.call == call) return idx;
    }

    try builder.append(allocator, AggregateSpec{
        .function = function,
        .argument = argument,
        .call = call,
    });
    return builder.items.len - 1;
}

fn registerAggregatesInExpression(
    allocator: std.mem.Allocator,
    expr: *const ast.Expression,
    builder: *std.ArrayListUnmanaged(AggregateSpec),
) Error!void {
    switch (expr.*) {
        .function_call => |*call| {
            if (aggregateFunctionFromName(call.name.canonical)) |_| {
                _ = try appendAggregateSpec(allocator, builder, call);
                return;
            }
            return Error.UnsupportedFunction;
        },
        .binary => |bin| {
            try registerAggregatesInExpression(allocator, bin.left, builder);
            try registerAggregatesInExpression(allocator, bin.right, builder);
        },
        .unary => |un| try registerAggregatesInExpression(allocator, un.operand, builder),
        .column, .literal, .star => {},
    }
}

fn parseCountArgument(call: *const ast.FunctionCall) Error!AggregateArgument {
    if (call.arguments.len == 0) return AggregateArgument.star;
    if (call.arguments.len == 1) {
        const arg = call.arguments[0];
        if (arg.* == .star) return AggregateArgument.star;
        return AggregateArgument{ .expression = arg };
    }
    return failFeature(.aggregate_argument_arity, call.span);
}

fn parseSingleArgument(call: *const ast.FunctionCall) Error!AggregateArgument {
    if (call.arguments.len != 1) return failFeature(.aggregate_argument_arity, call.span);
    const arg = call.arguments[0];
    if (arg.* == .star) return failFeature(.aggregate_argument_star, ast.expressionSpan(arg));
    return AggregateArgument{ .expression = arg };
}

fn aggregateFunctionFromName(name: []const u8) ?AggregateFunction {
    if (std.mem.eql(u8, name, "count")) return .count;
    if (std.mem.eql(u8, name, "sum")) return .sum;
    if (std.mem.eql(u8, name, "avg")) return .avg;
    if (std.mem.eql(u8, name, "min")) return .min;
    if (std.mem.eql(u8, name, "max")) return .max;
    return null;
}

fn findGroupColumnIndex(columns: []const GroupColumn, column: ast.ColumnRef, binding: ?TableBinding) ?usize {
    for (columns, 0..) |group_col, idx| {
        if (columnsEquivalent(group_col.column, column, binding)) return idx;
    }
    return null;
}

fn columnsEquivalent(a: ast.ColumnRef, b: ast.ColumnRef, binding: ?TableBinding) bool {
    if (!identifiersEqual(a.name, b.name)) return false;
    return qualifiersEqual(a.table, b.table, binding);
}

fn identifiersEqual(a: ast.Identifier, b: ast.Identifier) bool {
    if (a.quoted or b.quoted) {
        if (a.quoted != b.quoted) return false;
        return std.mem.eql(u8, a.value, b.value);
    }
    return std.mem.eql(u8, a.canonical, b.canonical);
}

fn qualifiersEqual(a: ?ast.Identifier, b: ?ast.Identifier, binding: ?TableBinding) bool {
    if (a) |lhs| {
        if (b) |rhs| {
            if (!identifiersEqual(lhs, rhs)) return false;
            return qualifierAllowed(binding, lhs);
        }
        // Allow qualified GROUP BY column to match unqualified reference when the qualifier is valid.
        return qualifierAllowed(binding, lhs);
    }
    if (b) |rhs| {
        return qualifierAllowed(binding, rhs);
    }
    return true;
}

fn encodeGroupValues(store: *GroupStore, allocator: std.mem.Allocator, values: []const event_mod.Value) std.mem.Allocator.Error![]u8 {
    if (values.len == 0) return emptyMutableSlice();

    var buffer = std.ArrayListUnmanaged(u8){};
    errdefer buffer.deinit(allocator);

    for (values) |value| {
        try encodeGroupValue(store, &buffer, allocator, value);
    }

    const owned = try buffer.toOwnedSlice(allocator);
    buffer = std.ArrayListUnmanaged(u8){};
    return owned;
}

fn encodeGroupValue(
    store: *GroupStore,
    buffer: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    value: event_mod.Value,
) std.mem.Allocator.Error!void {
    const tag = meta.activeTag(value);
    const tag_byte: u8 = @intCast(@intFromEnum(tag));
    try buffer.append(allocator, tag_byte);

    switch (value) {
        .string => |bytes| {
            const id = try store.internString(bytes);
            var id_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &id_buf, id, .little);
            try buffer.appendSlice(allocator, &id_buf);
        },
        .integer => |int_value| {
            var int_buf: [8]u8 = undefined;
            std.mem.writeInt(i64, &int_buf, int_value, .little);
            try buffer.appendSlice(allocator, &int_buf);
        },
        .float => |float_value| {
            const canonical = if (math.isNan(float_value))
                math.nan(f64)
            else if (float_value == 0.0)
                0.0
            else
                float_value;
            const bits: u64 = @bitCast(canonical);
            var float_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &float_buf, bits, .little);
            try buffer.appendSlice(allocator, &float_buf);
        },
        .boolean => |flag| {
            try buffer.append(allocator, if (flag) 1 else 0);
        },
        .null => {},
    }
}

const AggregateInput = union(enum) {
    count_all,
    value: event_mod.Value,
    skip,
};

fn updateCountState(
    txn: *Transaction,
    index: usize,
    state: *AggregateState,
    spec: AggregateSpec,
    input: AggregateInput,
) Error!void {
    _ = spec;
    switch (input) {
        .skip => {},
        .count_all, .value => {
            var count_state = &state.count;
            if (count_state.total == math.maxInt(u64)) return Error.ArithmeticOverflow;
            txn.record(.count, index, state);
            count_state.total += 1;
        },
    }
}

fn updateSumState(
    txn: *Transaction,
    index: usize,
    state: *AggregateState,
    spec: AggregateSpec,
    input: AggregateInput,
) Error!void {
    _ = spec;
    switch (input) {
        .skip => return,
        .count_all => unreachable,
        .value => |value| switch (value) {
            .integer => |int_value| {
                txn.record(.sum, index, state);
                var sum_state = &state.sum;
                if (!sum_state.has_value) {
                    sum_state.has_value = true;
                    sum_state.kind = .integer;
                    sum_state.integer_total = int_value;
                    return;
                }
                switch (sum_state.kind) {
                    .integer => {
                        const ov = @addWithOverflow(sum_state.integer_total, int_value);
                        if (ov[1] != 0) return Error.ArithmeticOverflow;
                        sum_state.integer_total = ov[0];
                    },
                    .float => sum_state.float_total += @as(f64, @floatFromInt(int_value)),
                }
            },
            .float => |float_value| {
                txn.record(.sum, index, state);
                var sum_state = &state.sum;
                if (!sum_state.has_value) {
                    sum_state.has_value = true;
                    sum_state.kind = .float;
                    sum_state.float_total = float_value;
                    return;
                }
                switch (sum_state.kind) {
                    .integer => {
                        sum_state.kind = .float;
                        sum_state.float_total = @as(f64, @floatFromInt(sum_state.integer_total)) + float_value;
                    },
                    .float => sum_state.float_total += float_value,
                }
            },
            else => return Error.TypeMismatch,
        },
    }
}

fn updateAvgState(
    txn: *Transaction,
    index: usize,
    state: *AggregateState,
    input: AggregateInput,
) Error!void {
    switch (input) {
        .skip => return,
        .count_all => unreachable,
        .value => |value| {
            switch (value) {
                .integer, .float => {},
                else => return Error.TypeMismatch,
            }

            txn.record(.avg, index, state);

            var avg_state = &state.avg;
            switch (value) {
                .integer => |int_value| {
                    if (!avg_state.sum.has_value) {
                        avg_state.sum.has_value = true;
                        avg_state.sum.kind = .integer;
                        avg_state.sum.integer_total = int_value;
                    } else switch (avg_state.sum.kind) {
                        .integer => {
                            const ov = @addWithOverflow(avg_state.sum.integer_total, int_value);
                            if (ov[1] != 0) return Error.ArithmeticOverflow;
                            avg_state.sum.integer_total = ov[0];
                        },
                        .float => avg_state.sum.float_total += @as(f64, @floatFromInt(int_value)),
                    }
                },
                .float => |float_value| {
                    if (!avg_state.sum.has_value) {
                        avg_state.sum.has_value = true;
                        avg_state.sum.kind = .float;
                        avg_state.sum.float_total = float_value;
                    } else switch (avg_state.sum.kind) {
                        .integer => {
                            avg_state.sum.kind = .float;
                            avg_state.sum.float_total = @as(f64, @floatFromInt(avg_state.sum.integer_total)) + float_value;
                        },
                        .float => avg_state.sum.float_total += float_value,
                    }
                },
                else => unreachable,
            }

            if (avg_state.count == math.maxInt(u64)) return Error.ArithmeticOverflow;
            avg_state.count += 1;
        },
    }
}

fn updateMinMaxState(
    txn: *Transaction,
    index: usize,
    state: *AggregateState,
    allocator: std.mem.Allocator,
    input: AggregateInput,
    mode: enum { min, max },
) Error!void {
    switch (input) {
        .skip => return,
        .count_all => unreachable,
        .value => |value| {
            const function = switch (mode) {
                .min => AggregateFunction.min,
                .max => AggregateFunction.max,
            };
            var target = switch (mode) {
                .min => &state.min,
                .max => &state.max,
            };

            if (!target.has_value) {
                const new_value = try copyValueOwned(allocator, value);
                txn.record(function, index, state);
                target.value = new_value;
                target.has_value = true;
                return;
            }

            const should_replace = switch (mode) {
                .min => try compareOrder(.greater, target.value, value),
                .max => try compareOrder(.less, target.value, value),
            };
            if (!should_replace) return;

            const new_value = try copyValueOwned(allocator, value);
            txn.record(function, index, state);
            target.value = new_value;
            target.has_value = true;
        },
    }
}

fn aggregateResultValue(state: *const AggregateState, spec: *const AggregateSpec) Error!event_mod.Value {
    return switch (spec.function) {
        .count => blk: {
            const total = state.count.total;
            if (total > math.maxInt(i64)) return Error.ArithmeticOverflow;
            break :blk event_mod.Value{ .integer = @intCast(total) };
        },
        .sum => blk: {
            if (!state.sum.has_value) break :blk event_mod.Value{ .null = {} };
            break :blk switch (state.sum.kind) {
                .integer => event_mod.Value{ .integer = state.sum.integer_total },
                .float => event_mod.Value{ .float = state.sum.float_total },
            };
        },
        .avg => blk: {
            if (state.avg.count == 0) break :blk event_mod.Value{ .null = {} };
            const total_float: f64 = switch (state.avg.sum.kind) {
                .integer => @as(f64, @floatFromInt(state.avg.sum.integer_total)),
                .float => state.avg.sum.float_total,
            };
            const average = total_float / @as(f64, @floatFromInt(state.avg.count));
            break :blk event_mod.Value{ .float = average };
        },
        .min => blk: {
            if (!state.min.has_value) break :blk event_mod.Value{ .null = {} };
            break :blk state.min.value;
        },
        .max => blk: {
            if (!state.max.has_value) break :blk event_mod.Value{ .null = {} };
            break :blk state.max.value;
        },
    };
}

fn toFloat(value: event_mod.Value) Error!f64 {
    return switch (value) {
        .integer => |v| @as(f64, @floatFromInt(v)),
        .float => |v| v,
        else => Error.TypeMismatch,
    };
}

const OrderOp = enum { less, less_equal, greater, greater_equal };

fn compareOrder(op: OrderOp, left: event_mod.Value, right: event_mod.Value) Error!bool {
    if (left == .null or right == .null) return false;

    if (left == .string and right == .string) {
        const cmp = std.mem.order(u8, left.string, right.string);
        return switch (op) {
            .less => cmp == .lt,
            .less_equal => cmp == .lt or cmp == .eq,
            .greater => cmp == .gt,
            .greater_equal => cmp == .gt or cmp == .eq,
        };
    }

    if (left == .integer and right == .integer) {
        const li = left.integer;
        const ri = right.integer;
        return switch (op) {
            .less => li < ri,
            .less_equal => li <= ri,
            .greater => li > ri,
            .greater_equal => li >= ri,
        };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);

    return switch (op) {
        .less => lf < rf,
        .less_equal => lf <= rf,
        .greater => lf > rf,
        .greater_equal => lf >= rf,
    };
}

fn compareEqual(left: event_mod.Value, right: event_mod.Value) bool {
    switch (left) {
        .integer => |l| switch (right) {
            .integer => |r| return l == r,
            .float => |r| return @as(f64, @floatFromInt(l)) == r,
            else => return false,
        },
        .float => |l| switch (right) {
            .integer => |r| return l == @as(f64, @floatFromInt(r)),
            .float => |r| {
                if (math.isNan(l) and math.isNan(r)) return true;
                return l == r;
            },
            else => return false,
        },
        .boolean => |l| switch (right) {
            .boolean => |r| return l == r,
            else => return false,
        },
        .string => |l| switch (right) {
            .string => |r| return std.mem.eql(u8, l, r),
            else => return false,
        },
        .null => switch (right) {
            .null => return true,
            else => return false,
        },
    }
}

fn valuesEqual(a: []const event_mod.Value, b: []const event_mod.Value) bool {
    if (a.len != b.len) return false;
    for (a, 0..) |value, idx| {
        if (!compareEqual(value, b[idx])) return false;
    }
    return true;
}

fn copyValueOwned(allocator: std.mem.Allocator, value: event_mod.Value) Error!event_mod.Value {
    return switch (value) {
        .string => |text| blk: {
            const buffer = try allocator.alloc(u8, text.len);
            if (text.len != 0) std.mem.copyForwards(u8, buffer, text);
            break :blk event_mod.Value{ .string = buffer };
        },
        else => value,
    };
}

fn freeOwnedValue(allocator: std.mem.Allocator, value: event_mod.Value) void {
    if (value == .string) {
        allocator.free(value.string);
    }
}

fn cloneRowValue(allocator: std.mem.Allocator, value: event_mod.Value) Error!event_mod.Value {
    return switch (value) {
        .string => |text| blk: {
            const buffer = try allocator.alloc(u8, text.len);
            if (text.len != 0) std.mem.copyForwards(u8, buffer, text);
            break :blk event_mod.Value{ .string = buffer };
        },
        else => value,
    };
}

fn identifierBinding(identifier: ast.Identifier) IdentifierBinding {
    return IdentifierBinding{
        .canonical = identifier.canonical,
        .value = identifier.value,
        .quoted = identifier.quoted,
    };
}

fn qualifierMatches(binding: IdentifierBinding, qualifier: ast.Identifier) bool {
    if (binding.quoted or qualifier.quoted) {
        if (binding.quoted != qualifier.quoted) return false;
        return std.mem.eql(u8, binding.value, qualifier.value);
    }
    return std.mem.eql(u8, binding.canonical, qualifier.canonical);
}

fn qualifierAllowed(binding_opt: ?TableBinding, qualifier: ast.Identifier) bool {
    if (binding_opt) |binding| {
        if (qualifierMatches(binding.table, qualifier)) return true;
        if (binding.alias) |alias_binding| {
            if (qualifierMatches(alias_binding, qualifier)) return true;
        }
    }
    return false;
}

fn determineTableBinding(stmt: *const ast.SelectStatement) Error!?TableBinding {
    if (stmt.from.len == 0) return null;

    const first = stmt.from[0];
    return switch (first.relation) {
        .table => |table_ref| blk: {
            var binding = TableBinding{ .table = identifierBinding(table_ref.name) };
            if (table_ref.alias) |alias_ident| {
                binding.alias = identifierBinding(alias_ident);
            }
            break :blk binding;
        },
        .derived => |derived| failFeature(.derived_table, derived.span),
    };
}

const EventResolverContext = struct {
    table_binding: ?TableBinding,
};

fn resolveEventBinding(context_ptr: *anyopaque, column: ast.ColumnRef) expr_ir.Error!expr_ir.EventBinding {
    const context: *EventResolverContext = @ptrCast(@alignCast(context_ptr));
    if (column.table) |qualifier| {
        if (!qualifierAllowed(context.table_binding, qualifier)) {
            return expr_ir.Error.UnknownColumn;
        }
    }

    const identifier = column.name;
    const canonical = identifier.canonical;

    if (!identifier.quoted and std.mem.eql(u8, canonical, "message")) {
        return expr_ir.EventBinding.message;
    }

    if (!identifier.quoted and std.mem.eql(u8, canonical, "source_id")) {
        return expr_ir.EventBinding.source_id;
    }

    return expr_ir.EventBinding{
        .field = .{
            .name = identifier.value,
            .canonical = identifier.canonical,
            .quoted = identifier.quoted,
        },
    };
}

const GroupResolverContext = struct {
    columns: []const GroupColumn,
    table_binding: ?TableBinding,
};

fn resolveGroupBinding(context_ptr: *anyopaque, column: ast.ColumnRef) expr_ir.Error!?usize {
    const context: *GroupResolverContext = @ptrCast(@alignCast(context_ptr));
    return findGroupColumnIndex(context.columns, column, context.table_binding);
}

const AggregateResolverContext = struct {
    specs: []const AggregateSpec,
};

fn resolveAggregateBinding(context_ptr: *anyopaque, call: *const ast.FunctionCall) expr_ir.Error!?usize {
    const context: *AggregateResolverContext = @ptrCast(@alignCast(context_ptr));
    for (context.specs, 0..) |spec, idx| {
        if (spec.call == call) return idx;
    }
    return null;
}

const EventAccessorContext = struct {
    event: *const event_mod.Event,
};

fn loadEventValue(context_ptr: *anyopaque, binding: expr_ir.EventBinding) expr_ir.Error!event_mod.Value {
    const context: *EventAccessorContext = @ptrCast(@alignCast(context_ptr));
    const event = context.event;

    return switch (event.payload) {
        .log => |log_event| switch (binding) {
            .message => event_mod.Value{ .string = log_event.message },
            .source_id => blk: {
                if (event.metadata.source_id) |source_id| {
                    break :blk event_mod.Value{ .string = source_id };
                }
                break :blk event_mod.Value{ .null = {} };
            },
            .field => |field| {
                for (log_event.fields) |log_field| {
                    if (field.quoted) {
                        if (std.mem.eql(u8, log_field.name, field.name)) {
                            return log_field.value;
                        }
                    } else if (ascii.eqlIgnoreCase(log_field.name, field.canonical)) {
                        return log_field.value;
                    }
                }
                return event_mod.Value{ .null = {} };
            },
        },
    };
}

const AggregateAccessorContext = struct {
    program: *Program,
    group: *const GroupState,
};

fn loadAggregateValue(context_ptr: *anyopaque, aggregate_index: usize) expr_ir.Error!event_mod.Value {
    const context: *AggregateAccessorContext = @ptrCast(@alignCast(context_ptr));
    if (aggregate_index >= context.group.aggregates.len) return expr_ir.Error.UnknownColumn;
    const spec = &context.program.plan.group_aggregate.aggregates[aggregate_index];
    return aggregateResultValue(&context.group.aggregates[aggregate_index], spec) catch |err| switch (err) {
        Error.RowTooLarge,
        Error.GroupStateTooLarge,
        Error.StateBudgetExceeded,
        Error.CpuBudgetExceeded,
        Error.LateEvent => unreachable,
        else => |other| return other,
    };
}

const testing = std.testing;

fn createEventWithSeverity(
    allocator: std.mem.Allocator,
    message: []const u8,
    severity: event_mod.Value,
) !event_mod.Event {
    const fields = try allocator.alloc(event_mod.Field, 1);
    fields[0] = .{ .name = "syslog_severity", .value = severity };

    return event_mod.Event{
        .metadata = .{ .source_id = "syslog" },
        .payload = .{
            .log = .{
                .message = message,
                .fields = fields,
            },
        },
    };
}

fn testEvent(allocator: std.mem.Allocator, message: []const u8, severity: i64) !event_mod.Event {
    return createEventWithSeverity(allocator, message, .{ .integer = severity });
}

fn freeEvent(allocator: std.mem.Allocator, ev: event_mod.Event) void {
    switch (ev.payload) {
        .log => |log_event| allocator.free(log_event.fields),
    }
}

fn createValueEvent(allocator: std.mem.Allocator, value: event_mod.Value) !event_mod.Event {
    const fields = try allocator.alloc(event_mod.Field, 1);
    fields[0] = .{ .name = "value", .value = value };

    return event_mod.Event{
        .metadata = .{ .source_id = "value_source" },
        .payload = .{
            .log = .{
                .message = "value_event",
                .fields = fields,
            },
        },
    };
}

fn nextTimestamp(counter: *u64) u64 {
    const value = counter.*;
    counter.* += 1;
    return value;
}

test "count all messages without groups" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT COUNT(*) AS total FROM logs");

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event1 = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event1);
    var event2 = try testEvent(testing.allocator, "world", 5);
    defer freeEvent(testing.allocator, event2);

    var clock: u64 = 1;

    const row1 = try program.execute(testing.allocator, &event1, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row1.deinit();
    try testing.expectEqual(@as(usize, 1), row1.values.len);
    try testing.expectEqualStrings("total", row1.values[0].name);
    try testing.expect(row1.values[0].value == .integer);
    try testing.expectEqual(@as(i64, 1), row1.values[0].value.integer);

    const row2 = try program.execute(testing.allocator, &event2, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row2.deinit();
    try testing.expectEqual(@as(i64, 2), row2.values[0].value.integer);
}

test "count per message with having filter" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY message
        \\HAVING COUNT(*) > 1
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event1 = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event1);
    var event2 = try testEvent(testing.allocator, "hello", 3);
    defer freeEvent(testing.allocator, event2);

    var clock: u64 = 1;

    const row1 = try program.execute(testing.allocator, &event1, nextTimestamp(&clock));
    try testing.expect(row1 == null);

    const row2 = try program.execute(testing.allocator, &event2, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row2.deinit();
    try testing.expectEqual(@as(usize, 2), row2.values.len);
    try testing.expectEqualStrings("message", row2.values[0].name);
    try testing.expectEqualStrings("hello", row2.values[0].value.string);
    try testing.expectEqual(@as(i64, 2), row2.values[1].value.integer);
}

test "null policy drops oversized row" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT COUNT(*) AS total FROM logs");

    var program = try compile(testing.allocator, stmt, .{
        .limits = .{ .max_row_bytes = 1 },
        .error_policy = .null,
    });
    defer program.deinit();

    var event = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event);

    var clock: u64 = 1;

    const produced = try program.execute(testing.allocator, &event, nextTimestamp(&clock));
    try testing.expect(produced == null);
    try testing.expect(program.takeRowAdjustment() == .nullified);
    try testing.expect(program.takeRowAdjustment() == .none);
}

test "having treats null aggregate as false" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, SUM(value) AS total
        \\FROM logs
        \\GROUP BY message
        \\HAVING SUM(value) > 0
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var null_event = try createValueEvent(testing.allocator, .{ .null = {} });
    defer freeEvent(testing.allocator, null_event);
    var value_event = try createValueEvent(testing.allocator, .{ .integer = 5 });
    defer freeEvent(testing.allocator, value_event);

    var clock: u64 = 1;

    const maybe_row_null = try program.execute(testing.allocator, &null_event, nextTimestamp(&clock));
    try testing.expect(maybe_row_null == null);
    try testing.expectEqual(@as(usize, 1), program.group_store.count());

    const row = try program.execute(testing.allocator, &value_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row.deinit();
    try testing.expectEqual(@as(usize, 2), row.values.len);
    try testing.expectEqualStrings("message", row.values[0].name);
    try testing.expectEqualStrings("value_event", row.values[0].value.string);
    try testing.expectEqual(@as(i64, 5), row.values[1].value.integer);
}

test "group by float treats negative zero as zero" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT value, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY value
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var zero_event = try createValueEvent(testing.allocator, .{ .float = 0.0 });
    defer freeEvent(testing.allocator, zero_event);
    var negative_zero_event = try createValueEvent(testing.allocator, .{ .float = -0.0 });
    defer freeEvent(testing.allocator, negative_zero_event);

    var clock: u64 = 1;

    const first_row = try program.execute(testing.allocator, &zero_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer first_row.deinit();
    try testing.expectEqual(@as(usize, 1), program.group_store.count());
    try testing.expectEqual(@as(usize, 2), first_row.values.len);
    try testing.expect(first_row.values[0].value == .float);
    try testing.expectEqual(@as(f64, 0.0), first_row.values[0].value.float);
    try testing.expectEqual(@as(i64, 1), first_row.values[1].value.integer);

    const second_row = try program.execute(testing.allocator, &negative_zero_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer second_row.deinit();
    try testing.expectEqual(@as(usize, 1), program.group_store.count());
    try testing.expectEqual(@as(usize, 2), second_row.values.len);
    try testing.expect(second_row.values[0].value == .float);
    try testing.expectEqual(@as(f64, 0.0), second_row.values[0].value.float);
    try testing.expectEqual(@as(i64, 2), second_row.values[1].value.integer);
}

test "group by float groups NaNs together" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT value, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY value
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var first_nan_event = try createValueEvent(testing.allocator, .{ .float = math.nan(f64) });
    defer freeEvent(testing.allocator, first_nan_event);
    var second_nan_event = try createValueEvent(testing.allocator, .{ .float = math.nan(f64) });
    defer freeEvent(testing.allocator, second_nan_event);

    var clock: u64 = 1;

    const first_row = try program.execute(testing.allocator, &first_nan_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer first_row.deinit();
    try testing.expectEqual(@as(usize, 1), program.group_store.count());
    try testing.expectEqual(@as(usize, 2), first_row.values.len);
    try testing.expect(first_row.values[0].value == .float);
    try testing.expect(math.isNan(first_row.values[0].value.float));
    try testing.expectEqual(@as(i64, 1), first_row.values[1].value.integer);

    const second_row = try program.execute(testing.allocator, &second_nan_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer second_row.deinit();
    try testing.expectEqual(@as(usize, 1), program.group_store.count());
    try testing.expectEqual(@as(usize, 2), second_row.values.len);
    try testing.expect(second_row.values[0].value == .float);
    try testing.expect(math.isNan(second_row.values[0].value.float));
    try testing.expectEqual(@as(i64, 2), second_row.values[1].value.integer);
}

test "enforces max groups when having filters rows" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY message
        \\HAVING COUNT(*) > 1
    );

    var program = try compile(testing.allocator, stmt, .{
        .eviction = .{
            .ttl_ns = null,
            .max_groups = 2,
            .sweep_interval_ns = 0,
        },
    });
    defer program.deinit();

    var event_a = try testEvent(testing.allocator, "a", 1);
    defer freeEvent(testing.allocator, event_a);
    var event_b = try testEvent(testing.allocator, "b", 1);
    defer freeEvent(testing.allocator, event_b);
    var event_c = try testEvent(testing.allocator, "c", 1);
    defer freeEvent(testing.allocator, event_c);

    var clock: u64 = 1;

    const maybe_row_a = try program.execute(testing.allocator, &event_a, nextTimestamp(&clock));
    try testing.expect(maybe_row_a == null);
    try testing.expectEqual(@as(usize, 1), program.group_store.count());

    const maybe_row_b = try program.execute(testing.allocator, &event_b, nextTimestamp(&clock));
    try testing.expect(maybe_row_b == null);
    try testing.expectEqual(@as(usize, 2), program.group_store.count());

    const maybe_row_c = try program.execute(testing.allocator, &event_c, nextTimestamp(&clock));
    try testing.expect(maybe_row_c == null);
    try testing.expectEqual(@as(usize, 2), program.group_store.count());
}

test "sum integer field" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT COUNT(*) AS total, SUM(syslog_severity) AS severity_sum
        \\FROM logs
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event1 = try testEvent(testing.allocator, "first", 2);
    defer freeEvent(testing.allocator, event1);
    var event2 = try testEvent(testing.allocator, "second", 3);
    defer freeEvent(testing.allocator, event2);

    var clock: u64 = 1;

    const row1 = try program.execute(testing.allocator, &event1, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row1.deinit();
    try testing.expectEqual(@as(i64, 1), row1.values[0].value.integer);
    try testing.expectEqual(@as(i64, 2), row1.values[1].value.integer);

    const row2 = try program.execute(testing.allocator, &event2, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row2.deinit();
    try testing.expectEqual(@as(usize, 2), row2.values.len);
    try testing.expectEqual(@as(i64, 2), row2.values[0].value.integer);
    try testing.expectEqual(@as(i64, 5), row2.values[1].value.integer);
}

test "rollback removes new group on error" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT SUM(value) AS total
        \\FROM logs
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var bad_event = try createValueEvent(testing.allocator, .{ .string = "oops" });
    defer freeEvent(testing.allocator, bad_event);

    var clock: u64 = 1;
    try testing.expectError(Error.TypeMismatch, program.execute(testing.allocator, &bad_event, nextTimestamp(&clock)));
    try testing.expectEqual(@as(usize, 0), program.group_store.count());
}

test "rollback preserves existing aggregates on error" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT SUM(value) AS total
        \\FROM logs
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var good_event = try createValueEvent(testing.allocator, .{ .integer = 2 });
    defer freeEvent(testing.allocator, good_event);

    var bad_event = try createValueEvent(testing.allocator, .{ .string = "bad" });
    defer freeEvent(testing.allocator, bad_event);

    var next_event = try createValueEvent(testing.allocator, .{ .integer = 3 });
    defer freeEvent(testing.allocator, next_event);

    var clock: u64 = 1;

    const first_row = try program.execute(testing.allocator, &good_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer first_row.deinit();
    try testing.expectEqual(@as(i64, 2), first_row.values[0].value.integer);

    try testing.expectError(Error.TypeMismatch, program.execute(testing.allocator, &bad_event, nextTimestamp(&clock)));
    try testing.expectEqual(@as(usize, 1), program.group_store.count());

    const maybe_row = try program.execute(testing.allocator, &next_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer maybe_row.deinit();
    try testing.expectEqual(@as(i64, 5), maybe_row.values[0].value.integer);
}

test "avg ignores nulls" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT AVG(syslog_severity) AS avg_severity
        \\FROM logs
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event_with_value = try testEvent(testing.allocator, "first", 4);
    defer freeEvent(testing.allocator, event_with_value);

    var event_null = try createEventWithSeverity(testing.allocator, "first_null", .{ .null = {} });
    defer freeEvent(testing.allocator, event_null);

    var clock: u64 = 1;

    const row1 = try program.execute(testing.allocator, &event_null, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row1.deinit();
    try testing.expect(row1.values[0].value == .null);

    const row2 = try program.execute(testing.allocator, &event_with_value, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row2.deinit();
    try testing.expectApproxEqAbs(@as(f64, 4.0), row2.values[0].value.float, 0.0001);
}

test "min and max update correctly" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT MIN(syslog_severity) AS min_sev, MAX(syslog_severity) AS max_sev
        \\FROM logs
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event_low = try testEvent(testing.allocator, "low", 1);
    defer freeEvent(testing.allocator, event_low);
    var event_high = try testEvent(testing.allocator, "high", 5);
    defer freeEvent(testing.allocator, event_high);

    var clock: u64 = 1;

    const row1 = try program.execute(testing.allocator, &event_high, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row1.deinit();
    try testing.expectEqual(@as(i64, 5), row1.values[1].value.integer);

    const row2 = try program.execute(testing.allocator, &event_low, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row2.deinit();
    try testing.expectEqual(@as(i64, 1), row2.values[0].value.integer);
    try testing.expectEqual(@as(i64, 5), row2.values[1].value.integer);
}

test "min and max preserve precision for large integers" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT MIN(syslog_severity) AS min_sev, MAX(syslog_severity) AS max_sev
        \\FROM logs
    );

    const base: i64 = 9_007_199_254_740_992;
    const larger = base + 1;

    {
        var program = try compile(testing.allocator, stmt, .{});
        defer program.deinit();

        var event_base = try testEvent(testing.allocator, "base", base);
        defer freeEvent(testing.allocator, event_base);
        var event_larger = try testEvent(testing.allocator, "larger", larger);
        defer freeEvent(testing.allocator, event_larger);

        var clock: u64 = 1;

        const row1 = try program.execute(testing.allocator, &event_base, nextTimestamp(&clock)) orelse return testing.expect(false);
        defer row1.deinit();
        try testing.expectEqual(base, row1.values[0].value.integer);
        try testing.expectEqual(base, row1.values[1].value.integer);

        const row2 = try program.execute(testing.allocator, &event_larger, nextTimestamp(&clock)) orelse return testing.expect(false);
        defer row2.deinit();
        try testing.expectEqual(base, row2.values[0].value.integer);
        try testing.expectEqual(larger, row2.values[1].value.integer);
    }

    {
        var program = try compile(testing.allocator, stmt, .{});
        defer program.deinit();

        var event_larger = try testEvent(testing.allocator, "larger_first", larger);
        defer freeEvent(testing.allocator, event_larger);
        var event_base = try testEvent(testing.allocator, "base_second", base);
        defer freeEvent(testing.allocator, event_base);

        var clock: u64 = 1;

        const row1 = try program.execute(testing.allocator, &event_larger, nextTimestamp(&clock)) orelse return testing.expect(false);
        defer row1.deinit();
        try testing.expectEqual(larger, row1.values[0].value.integer);
        try testing.expectEqual(larger, row1.values[1].value.integer);

        const row2 = try program.execute(testing.allocator, &event_base, nextTimestamp(&clock)) orelse return testing.expect(false);
        defer row2.deinit();
        try testing.expectEqual(base, row2.values[0].value.integer);
        try testing.expectEqual(larger, row2.values[1].value.integer);
    }
}

test "compareOrder preserves precision for large integers" {
    const base: i64 = 9_007_199_254_740_992;
    const larger = base + 1;

    try testing.expect(try compareOrder(.less, .{ .integer = base }, .{ .integer = larger }));
    try testing.expect(try compareOrder(.greater, .{ .integer = larger }, .{ .integer = base }));
    try testing.expect(try compareOrder(.less_equal, .{ .integer = base }, .{ .integer = base }));
    try testing.expect(try compareOrder(.greater_equal, .{ .integer = larger }, .{ .integer = larger }));
    try testing.expect(!try compareOrder(.less, .{ .integer = larger }, .{ .integer = base }));
}

test "compile distinct reports diagnostic" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const query = "SELECT DISTINCT message FROM logs";
    const stmt = try @import("parser.zig").parseSelect(arena, query);

    try testing.expectError(Error.UnsupportedFeature, compile(testing.allocator, stmt, .{}));

    const diag = takeFeatureGateDiagnostic() orelse return testing.expect(false);
    try testing.expectEqual(FeatureGate.distinct, diag.feature);
    try testing.expect(std.mem.eql(u8, query[diag.span.start..diag.span.end], "DISTINCT"));
}

test "compile order by reports diagnostic" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const query = "SELECT message FROM logs ORDER BY message DESC";
    const stmt = try @import("parser.zig").parseSelect(arena, query);

    try testing.expectError(Error.UnsupportedFeature, compile(testing.allocator, stmt, .{}));

    const diag = takeFeatureGateDiagnostic() orelse return testing.expect(false);
    try testing.expectEqual(FeatureGate.order_by, diag.feature);
    try testing.expect(std.mem.eql(u8, query[diag.span.start..diag.span.end], "ORDER BY message DESC"));
}

test "compile join reports diagnostic" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const query = "SELECT logs.message, COUNT(*) FROM logs JOIN devices ON logs.device_id = devices.id";
    const stmt = try @import("parser.zig").parseSelect(arena, query);

    try testing.expectError(Error.UnsupportedFeature, compile(testing.allocator, stmt, .{}));

    const diag = takeFeatureGateDiagnostic() orelse return testing.expect(false);
    try testing.expectEqual(FeatureGate.join, diag.feature);
    try testing.expect(std.mem.eql(u8, query[diag.span.start..diag.span.end], "JOIN devices ON logs.device_id = devices.id"));
}

test "reject non aggregate projections" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message
        \\FROM logs
        \\GROUP BY message
    );
    try testing.expectError(Error.UnsupportedFeature, compile(testing.allocator, stmt, .{}));
}

test "evicts stale groups after ttl" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY message
    );

    var program = try compile(testing.allocator, stmt, .{
        .eviction = .{
            .ttl_ns = 10,
            .max_groups = 10,
            .sweep_interval_ns = 1,
        },
    });
    defer program.deinit();

    var event = try testEvent(testing.allocator, "alpha", 4);
    defer freeEvent(testing.allocator, event);

    const first = try program.execute(testing.allocator, &event, 100) orelse return testing.expect(false);
    defer first.deinit();
    try testing.expectEqual(@as(i64, 1), first.values[1].value.integer);

    const second = try program.execute(testing.allocator, &event, 200) orelse return testing.expect(false);
    defer second.deinit();
    try testing.expectEqual(@as(i64, 1), second.values[1].value.integer);
}

test "evicts least recently used group when limit exceeded" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY message
    );

    var program = try compile(testing.allocator, stmt, .{
        .eviction = .{
            .ttl_ns = null,
            .max_groups = 2,
            .sweep_interval_ns = 0,
        },
    });
    defer program.deinit();

    var event_a = try testEvent(testing.allocator, "a", 1);
    defer freeEvent(testing.allocator, event_a);
    var event_b = try testEvent(testing.allocator, "b", 2);
    defer freeEvent(testing.allocator, event_b);
    var event_c = try testEvent(testing.allocator, "c", 3);
    defer freeEvent(testing.allocator, event_c);

    const row_a1 = try program.execute(testing.allocator, &event_a, 10) orelse return testing.expect(false);
    defer row_a1.deinit();
    try testing.expectEqual(@as(i64, 1), row_a1.values[1].value.integer);

    const row_b1 = try program.execute(testing.allocator, &event_b, 11) orelse return testing.expect(false);
    defer row_b1.deinit();
    try testing.expectEqual(@as(i64, 1), row_b1.values[1].value.integer);

    const row_c1 = try program.execute(testing.allocator, &event_c, 12) orelse return testing.expect(false);
    defer row_c1.deinit();
    try testing.expectEqual(@as(i64, 1), row_c1.values[1].value.integer);

    const row_a2 = try program.execute(testing.allocator, &event_a, 13) orelse return testing.expect(false);
    defer row_a2.deinit();
    try testing.expectEqual(@as(i64, 1), row_a2.values[1].value.integer);

    const row_b2 = try program.execute(testing.allocator, &event_b, 14) orelse return testing.expect(false);
    defer row_b2.deinit();
    try testing.expectEqual(@as(i64, 1), row_b2.values[1].value.integer);
}

test "group store releases interned strings on eviction" {
    var store = try GroupStore.init(testing.allocator, .{
        .ttl_ns = null,
        .max_groups = 1,
        .sweep_interval_ns = 0,
    });
    defer store.deinit();

    const key_a_bytes = "alpha";
    const values_a = [_]event_mod.Value{event_mod.Value{ .string = key_a_bytes }};
    const encoded_a = try encodeGroupValues(&store, testing.allocator, &values_a);
    defer if (encoded_a.len != 0) testing.allocator.free(encoded_a);
    const hash_a = hash.XxHash3.hash(0, encoded_a);
    const acquisition_a = try store.acquire(encoded_a, hash_a, 10);
    try testing.expect(acquisition_a.created);

    const key_b_bytes = "beta";
    const values_b = [_]event_mod.Value{event_mod.Value{ .string = key_b_bytes }};
    const encoded_b = try encodeGroupValues(&store, testing.allocator, &values_b);
    defer if (encoded_b.len != 0) testing.allocator.free(encoded_b);
    const hash_b = hash.XxHash3.hash(0, encoded_b);
    const acquisition_b = try store.acquire(encoded_b, hash_b, 20);
    try testing.expect(acquisition_b.created);

    try testing.expectEqual(@as(usize, 1), store.count());
    try testing.expect(store.string_table.get(key_a_bytes) == null);
    try testing.expect(store.string_table.get(key_b_bytes) != null);
    try testing.expect(store.string_pool.items.len >= 1);
    try testing.expectEqual(@as(usize, 0), store.string_pool.items[0].ref_count);
    try testing.expectEqual(@as(usize, 0), store.string_pool.items[0].bytes.len);

    store.remove(acquisition_b.index);

    try testing.expectEqual(@as(usize, 0), store.count());
    try testing.expect(store.string_table.get(key_b_bytes) == null);
    try testing.expect(store.string_pool.items.len >= 2);
    try testing.expectEqual(@as(usize, 0), store.string_pool.items[1].ref_count);
    try testing.expectEqual(@as(usize, 0), store.string_pool.items[1].bytes.len);
}

test "group store acquire returns state budget exceeded when entry evicted immediately" {
    var store = try GroupStore.init(testing.allocator, .{
        .ttl_ns = null,
        .max_groups = 0,
        .sweep_interval_ns = 0,
        .max_state_bytes = 4,
    });
    defer store.deinit();

    const values = [_]event_mod.Value{event_mod.Value{ .string = "alpha" }};
    const encoded = try encodeGroupValues(&store, testing.allocator, &values);
    defer if (encoded.len != 0) testing.allocator.free(encoded);
    const hash_value = hash.XxHash3.hash(0, encoded);

    const acquisition = store.acquire(encoded, hash_value, 10) catch |err| {
        try testing.expectEqual(error.StateBudgetExceeded, err);
        try testing.expectEqual(@as(usize, 0), store.count());
        try testing.expectEqual(@as(usize, 0), store.stateBytes());
        return;
    };
    defer store.remove(acquisition.index);
    try testing.expect(false);
}

test "group store releases interned strings on cache hit" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY message
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    const message = "alpha";
    var event = try testEvent(testing.allocator, message, 1);
    defer freeEvent(testing.allocator, event);

    var clock: u64 = 1;

    const row_first = try program.execute(testing.allocator, &event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row_first.deinit();

    var ref_count: usize = 0;
    for (program.group_store.string_pool.items) |entry| {
        if (entry.bytes.len != 0 and std.mem.eql(u8, entry.bytes, message)) {
            ref_count = entry.ref_count;
            break;
        }
    }
    try testing.expectEqual(@as(usize, 1), ref_count);

    const row_second = try program.execute(testing.allocator, &event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer row_second.deinit();

    ref_count = 0;
    for (program.group_store.string_pool.items) |entry| {
        if (entry.bytes.len != 0 and std.mem.eql(u8, entry.bytes, message)) {
            ref_count = entry.ref_count;
            break;
        }
    }
    try testing.expectEqual(@as(usize, 1), ref_count);
}

test "program execute fails when state budget smaller than single group footprint" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY message
    );

    var program = try compile(testing.allocator, stmt, .{
        .limits = .{ .max_state_bytes = 4 },
    });
    defer program.deinit();

    var event = try testEvent(testing.allocator, "alpha", 1);
    defer freeEvent(testing.allocator, event);

    try std.testing.expectError(error.StateBudgetExceeded, program.execute(testing.allocator, &event, 10));
    try testing.expectEqual(@as(usize, 0), program.group_store.count());
    try testing.expectEqual(@as(usize, 0), program.group_store.stateBytes());
}

test "cpu budget exceed rolls back aggregate state" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT COUNT(*) AS total
        \\FROM logs
        \\GROUP BY message
    );

    var program = try compile(testing.allocator, stmt, .{
        .limits = .{ .cpu_budget_ns_per_second = 1_000_000 },
    });
    defer program.deinit();

    var event = try testEvent(testing.allocator, "alpha", 1);
    defer freeEvent(testing.allocator, event);

    var clock: u64 = 1;
    var duration_ns: u64 = 0;
    const start_ok = std.time.nanoTimestamp();
    const first_row = try program.executeTracked(
        testing.allocator,
        &event,
        nextTimestamp(&clock),
        start_ok,
        &duration_ns,
    ) orelse return testing.expect(false);
    defer first_row.deinit();

    duration_ns = 0;
    const start_exceed = std.time.nanoTimestamp() - 10_000_000;
    try testing.expectError(
        Error.CpuBudgetExceeded,
        program.executeTracked(
            testing.allocator,
            &event,
            nextTimestamp(&clock),
            start_exceed,
            &duration_ns,
        ),
    );
    try testing.expect(duration_ns >= 1_000_000);

    var observed_total: ?u64 = null;
    for (program.group_store.entries.items) |entry| {
        if (!entry.occupied) continue;
        observed_total = entry.state.aggregates[0].count.total;
        break;
    }
    try testing.expectEqual(@as(?u64, 1), observed_total);
}

test "execute projection" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\WHERE syslog_severity <= 4
        \\GROUP BY message
    );

    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event);

    var clock: u64 = 1;
    const maybe_row = try program.execute(testing.allocator, &event, nextTimestamp(&clock));
    defer if (maybe_row) |row| row.deinit();
    try testing.expect(maybe_row != null);
    const row = maybe_row.?;
    try testing.expectEqual(@as(usize, 2), row.values.len);
    try testing.expectEqualStrings("message", row.values[0].name);
    try testing.expect(row.values[0].value == .string);
    try testing.expectEqualStrings("hello", row.values[0].value.string);
    try testing.expectEqualStrings("total", row.values[1].name);
    try testing.expect(row.values[1].value == .integer);
    try testing.expectEqual(@as(i64, 1), row.values[1].value.integer);
}

test "filter out non matching event" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs
        \\WHERE syslog_severity <= 2
        \\GROUP BY message
    );
    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event, 1);
    try testing.expect(maybe_row == null);
}

test "missing column yields null" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT SUM(missing) AS total
        \\FROM logs
    );
    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event, 1);
    defer if (maybe_row) |row| row.deinit();

    const row = maybe_row orelse return testing.expect(false);
    try testing.expectEqual(@as(usize, 1), row.values.len);
    try testing.expect(row.values[0].value == .null);
}

test "execute star projection expands fields" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT * FROM logs");
    try testing.expectError(Error.UnsupportedFeature, compile(testing.allocator, stmt, .{}));
}

test "resolve qualified column reference" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT logs.message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY logs.message
    );
    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event);

    const maybe_row = try program.execute(testing.allocator, &event, 1);
    defer if (maybe_row) |row| row.deinit();

    const row = maybe_row orelse return testing.expect(false);
    try testing.expectEqual(@as(usize, 2), row.values.len);
    try testing.expectEqualStrings("message", row.values[0].name);
    try testing.expect(row.values[0].value == .string);
    try testing.expectEqualStrings("hello", row.values[0].value.string);
    try testing.expectEqualStrings("total", row.values[1].name);
    try testing.expect(row.values[1].value == .integer);
    try testing.expectEqual(@as(i64, 1), row.values[1].value.integer);
}

test "group by matches qualified alias" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT message, COUNT(*) AS total
        \\FROM logs AS l
        \\GROUP BY l.message
    );
    var program = try compile(testing.allocator, stmt, .{});
    defer program.deinit();

    var event = try testEvent(testing.allocator, "hello", 4);
    defer freeEvent(testing.allocator, event);

    var clock: u64 = 1;

    const maybe_row = try program.execute(testing.allocator, &event, nextTimestamp(&clock));
    defer if (maybe_row) |row| row.deinit();

    const row = maybe_row orelse return testing.expect(false);
    try testing.expectEqual(@as(usize, 2), row.values.len);
    try testing.expectEqualStrings("message", row.values[0].name);
    try testing.expect(row.values[0].value == .string);
    try testing.expectEqualStrings("hello", row.values[0].value.string);
    try testing.expectEqualStrings("total", row.values[1].name);
    try testing.expect(row.values[1].value == .integer);
    try testing.expectEqual(@as(i64, 1), row.values[1].value.integer);
}

test "unknown table qualifier yields error" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena,
        \\SELECT foo.message, COUNT(*) AS total
        \\FROM logs
        \\GROUP BY foo.message
    );
    try testing.expectError(Error.UnknownColumn, compile(testing.allocator, stmt, .{}));
}

test "unknown star qualifier yields error" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT foo.* FROM logs");
    try testing.expectError(Error.UnsupportedFeature, compile(testing.allocator, stmt, .{}));
}

test "execute mixed star and expression projection" {
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();

    const stmt = try @import("parser.zig").parseSelect(arena, "SELECT *, syslog_severity + 1 AS severity_plus_one FROM logs");
    try testing.expectError(Error.UnsupportedFeature, compile(testing.allocator, stmt, .{}));
}
