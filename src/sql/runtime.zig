const std = @import("std");
const ast = @import("ast.zig");
const source_mod = @import("source");
const plan = @import("plan.zig");
const event_mod = source_mod.event;

const ascii = std.ascii;
const math = std.math;
const hash = std.hash;
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
};

const AggregateFunction = plan.AggregateFunction;
const AggregateArgument = plan.AggregateArgument;
const AggregateSpec = plan.AggregateSpec;
const GroupColumn = plan.GroupColumn;
const Projection = plan.Projection;
const GroupProjection = plan.GroupProjection;
const AggregateProjection = plan.AggregateProjection;

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

const GroupState = struct {
    key_values: []event_mod.Value,
    aggregates: []AggregateState,
    last_seen_ns: u64,
    key_hash: u64,

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

const GroupAcquisition = struct {
    index: usize,
    created: bool,
};

const ProjectedKeys = struct {
    allocator: std.mem.Allocator,
    values: []event_mod.Value,
    hash: u64,
    allocated: bool,

    fn empty(allocator: std.mem.Allocator) ProjectedKeys {
        return ProjectedKeys{
            .allocator = allocator,
            .values = &.{},
            .hash = 0,
            .allocated = false,
        };
    }

    fn fromValues(allocator: std.mem.Allocator, values: []event_mod.Value, hashed: u64) ProjectedKeys {
        return ProjectedKeys{
            .allocator = allocator,
            .values = values,
            .hash = hashed,
            .allocated = values.len != 0,
        };
    }

    fn deinit(self: *ProjectedKeys) void {
        if (self.allocated and self.values.len != 0) {
            self.allocator.free(self.values);
        }
        self.values = &.{};
        self.hash = 0;
        self.allocated = false;
    }
};

const Transaction = struct {
    program: *Program,
    group_index: usize,
    created: bool,
    snapshot: []AggregateState,
    active: bool = true,

    fn init(program: *Program, group_index: usize, created: bool, snapshot: []AggregateState) Transaction {
        return Transaction{
            .program = program,
            .group_index = group_index,
            .created = created,
            .snapshot = snapshot,
            .active = true,
        };
    }

    fn commit(self: *Transaction, now_ns: u64) void {
        if (!self.active) return;
        if (self.created) {
            self.program.groups.items[self.group_index].last_seen_ns = now_ns;
        } else {
            self.program.replaceGroupAggregates(self.group_index, self.snapshot);
            self.program.groups.items[self.group_index].last_seen_ns = now_ns;
            self.snapshot = &.{};
        }
        self.active = false;
    }

    fn rollback(self: *Transaction) void {
        if (!self.active) return;
        if (self.created) {
            self.program.removeGroupAt(self.group_index);
        } else {
            self.program.destroyAggregateStateSlice(self.snapshot);
            self.snapshot = &.{};
        }
        self.active = false;
    }

    fn release(self: *Transaction) void {
        if (!self.active) return;
        self.rollback();
    }
};

pub const EvictionConfig = struct {
    ttl_ns: ?u64 = 15 * std.time.ns_per_min,
    max_groups: usize = 50_000,
    sweep_interval_ns: u64 = 60 * std.time.ns_per_s,
};

pub const CompileOptions = struct {
    eviction: EvictionConfig = .{},
};

/// Compiled representation of a SQL `SELECT` statement that can be executed
/// against pipeline events.
pub const Program = struct {
    allocator: std.mem.Allocator,
    plan: plan.PhysicalPlan,
    table_binding: ?TableBinding,
    groups: std.ArrayListUnmanaged(GroupState) = .{},
    eviction: EvictionConfig,
    last_sweep: u64 = 0,

    /// Releases memory allocated for the program metadata and accumulated state.
    pub fn deinit(self: *Program) void {
        for (self.groups.items) |*group| {
            group.deinit(self.allocator);
        }
        self.groups.deinit(self.allocator);

        self.allocator.free(self.plan.project.projections);
        self.allocator.free(self.plan.project.group_columns);
        self.allocator.free(self.plan.group_aggregate.aggregates);
    }

    /// Clears accumulated aggregation state, preserving compiled metadata.
    pub fn reset(self: *Program) void {
        self.last_sweep = 0;
        for (self.groups.items) |*group| {
            group.deinit(self.allocator);
        }
        self.groups.clearRetainingCapacity();
    }

    /// Evaluates the program against a single event, returning an optional row.
    /// When the `WHERE` or `HAVING` clause filters out the event, `null` is returned.
    pub fn execute(
        self: *Program,
        allocator: std.mem.Allocator,
        event: *const event_mod.Event,
        now_ns: u64,
    ) Error!?Row {
        const binding = self.table_binding;

        self.maybeSweep(now_ns);

        if (self.plan.filter) |filter_stage| {
            const predicate = try evaluateBoolean(filter_stage.predicate, event, binding);
            if (!predicate) return null;
        }

        var projected_keys = try self.runProjectStage(allocator, event, binding);
        defer projected_keys.deinit();

        const acquisition = try self.runGroupAggregateStage(&projected_keys, now_ns);
        var group_ptr = &self.groups.items[acquisition.index];

        var cloned_states: []AggregateState = &.{};
        if (!acquisition.created and group_ptr.aggregates.len != 0) {
            cloned_states = try self.cloneAggregateStates(group_ptr.aggregates);
        }

        var txn = Transaction.init(self, acquisition.index, acquisition.created, cloned_states);
        defer txn.release();

        var working_group_storage = GroupState{
            .key_values = group_ptr.key_values,
            .aggregates = if (acquisition.created) group_ptr.aggregates else cloned_states,
            .last_seen_ns = group_ptr.last_seen_ns,
            .key_hash = group_ptr.key_hash,
        };
        const working_group = if (acquisition.created)
            group_ptr
        else
            &working_group_storage;

        try self.updateAggregates(working_group, event, binding);
        working_group.last_seen_ns = now_ns;

        if (self.plan.having) |having_stage| {
            const keep = try self.evaluateHaving(having_stage.predicate, working_group);
            if (!keep) {
                txn.commit(now_ns);
                self.trimToLimit();
                return null;
            }
        }

        txn.commit(now_ns);
        group_ptr = &self.groups.items[acquisition.index];
        group_ptr.key_hash = projected_keys.hash;

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
        self.trimToLimit();
        var row = Row{ .allocator = allocator, .values = values, .owns_strings = true };

        if (self.plan.window) |window_stage| {
            try self.runWindowStage(window_stage, &row, now_ns);
        }

        if (self.plan.route) |route_stage| {
            try self.runRouteStage(route_stage, &row);
        }

        return row;
    }

    fn runProjectStage(self: *Program, allocator: std.mem.Allocator, event: *const event_mod.Event, binding: ?TableBinding) Error!ProjectedKeys {
        const group_columns = self.plan.project.group_columns;
        if (group_columns.len == 0) {
            return ProjectedKeys.empty(allocator);
        }

        var values = try allocator.alloc(event_mod.Value, group_columns.len);
        var ok = false;
        errdefer if (!ok) allocator.free(values);

        for (group_columns, 0..) |group_col, idx| {
            values[idx] = try resolveColumn(event, group_col.column, binding);
        }

        const hashed = hashGroupValues(values);
        ok = true;
        return ProjectedKeys.fromValues(allocator, values, hashed);
    }

    fn runGroupAggregateStage(self: *Program, projected: *const ProjectedKeys, now_ns: u64) Error!GroupAcquisition {
        if (projected.values.len == 0) {
            return self.stateForGlobalGroup(now_ns);
        }

        if (self.findGroup(projected.values, projected.hash)) |existing| {
            return GroupAcquisition{ .index = existing, .created = false };
        }

        return self.createGroup(projected.values, projected.hash, now_ns);
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

    fn stateForGlobalGroup(self: *Program, now_ns: u64) Error!GroupAcquisition {
        if (self.groups.items.len != 0) {
            return GroupAcquisition{ .index = 0, .created = false };
        }

        const aggregates = try self.createAggregateStateSlice();
        const new_group = try self.groups.addOne(self.allocator);
        new_group.* = GroupState{
            .key_values = &.{},
            .aggregates = aggregates,
            .last_seen_ns = now_ns,
            .key_hash = 0,
        };
        return GroupAcquisition{ .index = self.groups.items.len - 1, .created = true };
    }

    fn findGroup(self: *Program, values: []const event_mod.Value, hash_value: u64) ?usize {
        for (self.groups.items, 0..) |*group, idx| {
            if (group.key_hash != hash_value) continue;
            if (valuesEqual(group.key_values, values)) {
                return idx;
            }
        }
        return null;
    }

    fn createGroup(self: *Program, values: []const event_mod.Value, key_hash: u64, now_ns: u64) Error!GroupAcquisition {
        const copied = try self.copyKeyValues(values);
        errdefer {
            for (copied) |value| freeOwnedValue(self.allocator, value);
            self.allocator.free(copied);
        }

        const aggregates = try self.createAggregateStateSlice();
        errdefer {
            for (aggregates) |*agg| agg.deinit(self.allocator);
            self.allocator.free(aggregates);
        }

        const group_ptr = try self.groups.addOne(self.allocator);
        group_ptr.* = GroupState{
            .key_values = copied,
            .aggregates = aggregates,
            .last_seen_ns = now_ns,
            .key_hash = key_hash,
        };
        return GroupAcquisition{ .index = self.groups.items.len - 1, .created = true };
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

    fn updateAggregates(self: *Program, group: *GroupState, event: *const event_mod.Event, binding: ?TableBinding) Error!void {
        for (self.plan.group_aggregate.aggregates, 0..) |spec, idx| {
            const input = try evaluateAggregateArgument(spec, event, binding);
            var state = &group.aggregates[idx];
            switch (spec.function) {
                .count => try updateCountState(&state.count, spec, input),
                .sum => try updateSumState(&state.sum, spec, input),
                .avg => try updateAvgState(&state.avg, spec, input),
                .min => try updateMinMaxState(&state.min, self.allocator, spec, input, .min),
                .max => try updateMinMaxState(&state.max, self.allocator, spec, input, .max),
            }
        }
    }

    fn evaluateHaving(self: *Program, expr: *const ast.Expression, group: *const GroupState) Error!bool {
        const value = try self.evaluateAggregateExpression(expr, group);
        return switch (value) {
            .boolean => |b| b,
            .null => false,
            else => Error.TypeMismatch,
        };
    }

    fn evaluateAggregateExpression(self: *Program, expr: *const ast.Expression, group: *const GroupState) Error!event_mod.Value {
        return switch (expr.*) {
            .literal => |lit| valueFromLiteral(lit),
            .column => |col| self.resolveGroupColumn(group, col),
            .unary => |un| {
                const operand = try self.evaluateAggregateExpression(un.operand, group);
                return applyUnary(un.op, operand);
            },
            .binary => |bin| {
                const left = try self.evaluateAggregateExpression(bin.left, group);
                const right = try self.evaluateAggregateExpression(bin.right, group);
                return applyBinary(bin.op, left, right);
            },
            .function_call => |*call| {
                const index = self.aggregateIndexForCall(call) orelse return Error.UnsupportedFunction;
                const spec = &self.plan.group_aggregate.aggregates[index];
                return aggregateResultValue(&group.aggregates[index], spec);
            },
            .star => Error.UnsupportedFeature,
        };
    }

    fn resolveGroupColumn(self: *Program, group: *const GroupState, col: ast.ColumnRef) Error!event_mod.Value {
        for (self.plan.project.group_columns, 0..) |group_col, idx| {
            if (columnsEquivalent(group_col.column, col, self.table_binding)) {
                return group.key_values[idx];
            }
        }
        return Error.UnknownColumn;
    }

    fn aggregateIndexForCall(self: *Program, call: *const ast.FunctionCall) ?usize {
        for (self.plan.group_aggregate.aggregates, 0..) |spec, idx| {
            if (spec.call == call) return idx;
        }
        return null;
    }

    fn maybeSweep(self: *Program, now_ns: u64) void {
        const interval = self.eviction.sweep_interval_ns;
        if (interval == 0) return;
        if (self.last_sweep != 0 and now_ns - self.last_sweep < interval) return;
        self.sweep(now_ns);
    }

    fn sweep(self: *Program, now_ns: u64) void {
        if (self.eviction.ttl_ns) |ttl| {
            var idx: usize = 0;
            while (idx < self.groups.items.len) {
                const group = self.groups.items[idx];
                if (now_ns - group.last_seen_ns > ttl) {
                    var removed = self.groups.swapRemove(idx);
                    removed.deinit(self.allocator);
                    continue;
                }
                idx += 1;
            }
        }
        self.last_sweep = now_ns;
        self.trimToLimit();
    }

    fn removeGroupAt(self: *Program, index: usize) void {
        var removed = self.groups.swapRemove(index);
        removed.deinit(self.allocator);
    }

    fn destroyAggregateStateSlice(self: *Program, states: []AggregateState) void {
        if (states.len == 0) return;
        for (states) |*agg| {
            agg.deinit(self.allocator);
        }
        self.allocator.free(states);
    }

    fn replaceGroupAggregates(self: *Program, index: usize, new_states: []AggregateState) void {
        var group = &self.groups.items[index];
        self.destroyAggregateStateSlice(group.aggregates);
        group.aggregates = new_states;
    }

    fn cloneAggregateStates(self: *Program, states: []AggregateState) Error![]AggregateState {
        if (states.len == 0) return &.{};
        var clones = try self.allocator.alloc(AggregateState, states.len);
        var produced: usize = 0;
        errdefer {
            while (produced > 0) {
                produced -= 1;
                clones[produced].deinit(self.allocator);
            }
            self.allocator.free(clones);
        }
        for (states, 0..) |*state, idx| {
            clones[idx] = try self.cloneAggregateState(state);
            produced += 1;
        }
        return clones;
    }

    fn cloneAggregateState(self: *Program, state: *const AggregateState) Error!AggregateState {
        return switch (state.*) {
            .count => |value| AggregateState{ .count = value },
            .sum => |value| AggregateState{ .sum = value },
            .avg => |value| AggregateState{ .avg = value },
            .min => |value| AggregateState{ .min = try self.cloneMinMaxState(value) },
            .max => |value| AggregateState{ .max = try self.cloneMinMaxState(value) },
        };
    }

    fn cloneMinMaxState(self: *Program, state: MinMaxState) Error!MinMaxState {
        if (!state.has_value) {
            return MinMaxState{ .value = .{ .null = {} }, .has_value = false };
        }
        return MinMaxState{
            .value = try copyValueOwned(self.allocator, state.value),
            .has_value = true,
        };
    }

    fn trimToLimit(self: *Program) void {
        const limit = self.eviction.max_groups;
        if (limit == 0) return;
        while (self.groups.items.len > limit) {
            const idx = self.oldestGroupIndex();
            var removed = self.groups.swapRemove(idx);
            removed.deinit(self.allocator);
        }
    }

    fn oldestGroupIndex(self: *Program) usize {
        var oldest_index: usize = 0;
        var oldest_seen = self.groups.items[0].last_seen_ns;
        var idx: usize = 1;
        while (idx < self.groups.items.len) : (idx += 1) {
            const group = self.groups.items[idx];
            if (group.last_seen_ns < oldest_seen) {
                oldest_seen = group.last_seen_ns;
                oldest_index = idx;
            }
        }
        return oldest_index;
    }
};

/// Result row consumed by sink implementations.
pub const Row = struct {
    allocator: std.mem.Allocator,
    values: []ValueEntry,
    owns_strings: bool = false,

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

/// Named value produced by SQL execution.
pub const ValueEntry = struct {
    name: []const u8,
    value: event_mod.Value,
};

pub fn compile(allocator: std.mem.Allocator, stmt: *const ast.SelectStatement, options: CompileOptions) Error!Program {
    if (stmt.distinct) return Error.UnsupportedFeature;
    if (stmt.order_by.len != 0) return Error.UnsupportedFeature;
    if (stmt.from.len > 1) return Error.UnsupportedFeature;

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
            else => return Error.UnsupportedFeature,
        }
    }

    var aggregates_builder = std.ArrayListUnmanaged(AggregateSpec){};
    defer aggregates_builder.deinit(allocator);

    var projection_builder = std.ArrayListUnmanaged(Projection){};
    defer projection_builder.deinit(allocator);

    for (stmt.projection) |item| {
        switch (item.kind) {
            .star => return Error.UnsupportedFeature,
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
                        return Error.UnsupportedFeature;
                    };
                    const label = if (item.alias) |alias_ident| alias_ident.text() else column.name.text();
                    try projection_builder.append(allocator, .{
                        .group = .{
                            .label = label,
                            .column_index = index,
                        },
                    });
                } else {
                    return Error.UnsupportedFeature;
                }
            },
        }
    }

    if (aggregates_builder.items.len == 0) {
        return Error.UnsupportedFeature; // stateful engine requires at least one aggregate
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

    const physical_plan = plan.PhysicalPlan{
        .filter = filter_stage,
        .project = project_stage,
        .group_aggregate = aggregate_stage,
        .having = having_stage,
        .window = null,
        .route = null,
    };

    const program = Program{
        .allocator = allocator,
        .plan = physical_plan,
        .table_binding = table_binding,
        .groups = .{},
        .eviction = options.eviction,
    };
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
    if (call.distinct) return Error.UnsupportedFeature;

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
    return Error.UnsupportedFeature;
}

fn parseSingleArgument(call: *const ast.FunctionCall) Error!AggregateArgument {
    if (call.arguments.len != 1) return Error.UnsupportedFeature;
    const arg = call.arguments[0];
    if (arg.* == .star) return Error.UnsupportedFeature;
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

fn hashGroupValues(values: []const event_mod.Value) u64 {
    var hasher = hash.Fnv1a_64.init();
    for (values) |value| {
        hashGroupValue(&hasher, value);
    }
    return hasher.final();
}

fn hashGroupValue(hasher: *hash.Fnv1a_64, value: event_mod.Value) void {
    const tag = meta.activeTag(value);
    const tag_byte: u8 = @intCast(@intFromEnum(tag));
    hasher.update(&[_]u8{tag_byte});
    switch (value) {
        .string => |bytes| {
            hasher.update(bytes);
        },
        .integer => |int_value| {
            var buffer: [8]u8 = undefined;
            std.mem.writeInt(i64, &buffer, int_value, .little);
            hasher.update(&buffer);
        },
        .float => |float_value| {
            const canonical = if (math.isNan(float_value))
                math.nan(f64)
            else if (float_value == 0.0)
                0.0
            else
                float_value;
            const bits: u64 = @bitCast(canonical);
            var buffer: [8]u8 = undefined;
            std.mem.writeInt(u64, &buffer, bits, .little);
            hasher.update(&buffer);
        },
        .boolean => |flag| {
            const byte: u8 = if (flag) 1 else 0;
            hasher.update(&[_]u8{byte});
        },
        .null => {
            hasher.update(&[_]u8{0xff});
        },
    }
}

fn evaluateAggregateArgument(
    spec: AggregateSpec,
    event: *const event_mod.Event,
    binding: ?TableBinding,
) Error!AggregateInput {
    return switch (spec.argument) {
        .star => AggregateInput.count_all,
        .expression => |expr| blk: {
            const value = try evaluateExpression(expr, event, binding);
            if (value == .null and spec.function != .count) {
                break :blk AggregateInput.skip;
            }
            if (spec.function == .count and value == .null) {
                break :blk AggregateInput.skip;
            }
            break :blk AggregateInput{ .value = value };
        },
    };
}

const AggregateInput = union(enum) {
    count_all,
    value: event_mod.Value,
    skip,
};

fn updateCountState(state: *CountState, spec: AggregateSpec, input: AggregateInput) Error!void {
    _ = spec;
    switch (input) {
        .count_all, .value => {
            if (state.total == math.maxInt(u64)) return Error.ArithmeticOverflow;
            state.total += 1;
        },
        .skip => {},
    }
}

fn updateSumState(state: *SumState, spec: AggregateSpec, input: AggregateInput) Error!void {
    _ = spec;
    switch (input) {
        .skip => return,
        .count_all => unreachable,
        .value => |value| {
            switch (value) {
                .integer => |int_value| {
                    if (!state.has_value) {
                        state.has_value = true;
                        state.kind = .integer;
                        state.integer_total = int_value;
                        return;
                    }
                    switch (state.kind) {
                        .integer => {
                            const ov = @addWithOverflow(state.integer_total, int_value);
                            if (ov[1] != 0) return Error.ArithmeticOverflow;
                            state.integer_total = ov[0];
                        },
                        .float => {
                            state.float_total += @as(f64, @floatFromInt(int_value));
                        },
                    }
                },
                .float => |float_value| {
                    if (!state.has_value) {
                        state.has_value = true;
                        state.kind = .float;
                        state.float_total = float_value;
                        return;
                    }
                    switch (state.kind) {
                        .integer => {
                            state.kind = .float;
                            state.float_total = @as(f64, @floatFromInt(state.integer_total)) + float_value;
                        },
                        .float => state.float_total += float_value,
                    }
                },
                else => return Error.TypeMismatch,
            }
        },
    }
}

fn updateAvgState(state: *AvgState, spec: AggregateSpec, input: AggregateInput) Error!void {
    switch (input) {
        .skip => return,
        .count_all => unreachable,
        .value => |value| {
            try updateSumState(&state.sum, spec, AggregateInput{ .value = value });
            if (state.count == math.maxInt(u64)) return Error.ArithmeticOverflow;
            state.count += 1;
        },
    }
}

fn updateMinMaxState(
    state: *MinMaxState,
    allocator: std.mem.Allocator,
    spec: AggregateSpec,
    input: AggregateInput,
    mode: enum { min, max },
) Error!void {
    _ = spec;
    switch (input) {
        .skip => return,
        .count_all => unreachable,
        .value => |value| {
            if (!state.has_value) {
                state.value = try copyValueOwned(allocator, value);
                state.has_value = true;
                return;
            }
            const should_replace = switch (mode) {
                .min => try compareOrder(.greater, state.value, value),
                .max => try compareOrder(.less, state.value, value),
            };
            if (should_replace) {
                freeOwnedValue(allocator, state.value);
                state.value = try copyValueOwned(allocator, value);
                state.has_value = true;
            }
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

fn valueFromLiteral(lit: ast.Literal) Error!event_mod.Value {
    return switch (lit.value) {
        .integer => |value| event_mod.Value{ .integer = value },
        .float => |text| {
            const parsed = std.fmt.parseFloat(f64, text) catch return Error.TypeMismatch;
            return event_mod.Value{ .float = parsed };
        },
        .string => |text| event_mod.Value{ .string = text },
        .boolean => |b| event_mod.Value{ .boolean = b },
        .null => event_mod.Value{ .null = {} },
    };
}

fn resolveColumn(event: *const event_mod.Event, col: ast.ColumnRef, binding: ?TableBinding) Error!event_mod.Value {
    if (col.table) |qualifier| {
        if (!qualifierAllowed(binding, qualifier)) {
            return Error.UnknownColumn;
        }
    }

    return switch (event.payload) {
        .log => |log_event| resolveLogColumn(event, log_event, col),
    };
}

fn resolveLogColumn(ev: *const event_mod.Event, log_event: event_mod.LogEvent, col: ast.ColumnRef) Error!event_mod.Value {
    const identifier = col.name;
    const canonical = identifier.canonical;

    if (!identifier.quoted and std.mem.eql(u8, canonical, "message")) {
        return event_mod.Value{ .string = log_event.message };
    }

    if (!identifier.quoted and std.mem.eql(u8, canonical, "source_id")) {
        if (ev.metadata.source_id) |source_id| {
            return event_mod.Value{ .string = source_id };
        }
        return event_mod.Value{ .null = {} };
    }

    for (log_event.fields) |field| {
        if (identifier.quoted) {
            if (std.mem.eql(u8, field.name, identifier.value)) {
                return field.value;
            }
        } else if (ascii.eqlIgnoreCase(field.name, canonical)) {
            return field.value;
        }
    }

    return event_mod.Value{ .null = {} };
}

fn evaluateBoolean(expr: *const ast.Expression, event: *const event_mod.Event, binding: ?TableBinding) Error!bool {
    const value = try evaluateExpression(expr, event, binding);
    return switch (value) {
        .boolean => |b| b,
        else => Error.TypeMismatch,
    };
}

fn evaluateExpression(expr: *const ast.Expression, event: *const event_mod.Event, binding: ?TableBinding) Error!event_mod.Value {
    return switch (expr.*) {
        .literal => |lit| valueFromLiteral(lit),
        .column => |col| resolveColumn(event, col, binding),
        .unary => |unary| try evalUnary(unary, event, binding),
        .binary => |binary| try evalBinary(binary, event, binding),
        .function_call => |_| Error.UnsupportedFunction,
        .star => Error.UnsupportedFeature,
    };
}

fn evalUnary(unary: ast.UnaryExpr, event: *const event_mod.Event, binding: ?TableBinding) Error!event_mod.Value {
    const operand = try evaluateExpression(unary.operand, event, binding);
    return applyUnary(unary.op, operand);
}

fn applyUnary(op: ast.UnaryOperator, operand: event_mod.Value) Error!event_mod.Value {
    return switch (op) {
        .plus => operand,
        .minus => switch (operand) {
            .integer => |value| event_mod.Value{ .integer = -value },
            .float => |value| event_mod.Value{ .float = -value },
            else => Error.TypeMismatch,
        },
        .not => switch (operand) {
            .boolean => |value| event_mod.Value{ .boolean = !value },
            else => Error.TypeMismatch,
        },
    };
}

fn evalBinary(binary: ast.BinaryExpr, event: *const event_mod.Event, binding: ?TableBinding) Error!event_mod.Value {
    const left = try evaluateExpression(binary.left, event, binding);
    const right = try evaluateExpression(binary.right, event, binding);
    return applyBinary(binary.op, left, right);
}

fn applyBinary(op: ast.BinaryOperator, left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    return switch (op) {
        .add => try addValues(left, right),
        .subtract => try subtractValues(left, right),
        .multiply => try multiplyValues(left, right),
        .divide => try divideValues(left, right),
        .equal => event_mod.Value{ .boolean = compareEqual(left, right) },
        .not_equal => event_mod.Value{ .boolean = !compareEqual(left, right) },
        .less => event_mod.Value{ .boolean = try compareOrder(.less, left, right) },
        .less_equal => event_mod.Value{ .boolean = try compareOrder(.less_equal, left, right) },
        .greater => event_mod.Value{ .boolean = try compareOrder(.greater, left, right) },
        .greater_equal => event_mod.Value{ .boolean = try compareOrder(.greater_equal, left, right) },
        .logical_and => event_mod.Value{ .boolean = try logicalAnd(left, right) },
        .logical_or => event_mod.Value{ .boolean = try logicalOr(left, right) },
    };
}

fn addValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    if (left == .integer and right == .integer) {
        const ov = @addWithOverflow(left.integer, right.integer);
        if (ov[1] != 0) return Error.ArithmeticOverflow;
        return event_mod.Value{ .integer = ov[0] };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);
    return event_mod.Value{ .float = lf + rf };
}

fn subtractValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    if (left == .integer and right == .integer) {
        const ov = @subWithOverflow(left.integer, right.integer);
        if (ov[1] != 0) return Error.ArithmeticOverflow;
        return event_mod.Value{ .integer = ov[0] };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);
    return event_mod.Value{ .float = lf - rf };
}

fn multiplyValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    if (left == .integer and right == .integer) {
        const ov = @mulWithOverflow(left.integer, right.integer);
        if (ov[1] != 0) return Error.ArithmeticOverflow;
        return event_mod.Value{ .integer = ov[0] };
    }

    const lf = try toFloat(left);
    const rf = try toFloat(right);
    return event_mod.Value{ .float = lf * rf };
}

fn divideValues(left: event_mod.Value, right: event_mod.Value) Error!event_mod.Value {
    const denominator = try toFloat(right);
    if (denominator == 0) return Error.DivideByZero;
    const numerator = try toFloat(left);
    return event_mod.Value{ .float = numerator / denominator };
}

fn logicalAnd(left: event_mod.Value, right: event_mod.Value) Error!bool {
    if (left != .boolean or right != .boolean) return Error.TypeMismatch;
    return left.boolean and right.boolean;
}

fn logicalOr(left: event_mod.Value, right: event_mod.Value) Error!bool {
    if (left != .boolean or right != .boolean) return Error.TypeMismatch;
    return left.boolean or right.boolean;
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
        else => Error.UnsupportedFeature,
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
    try testing.expectEqual(@as(usize, 1), program.groups.items.len);

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
    try testing.expectEqual(@as(usize, 1), program.groups.items.len);
    try testing.expectEqual(@as(usize, 2), first_row.values.len);
    try testing.expect(first_row.values[0].value == .float);
    try testing.expectEqual(@as(f64, 0.0), first_row.values[0].value.float);
    try testing.expectEqual(@as(i64, 1), first_row.values[1].value.integer);

    const second_row = try program.execute(testing.allocator, &negative_zero_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer second_row.deinit();
    try testing.expectEqual(@as(usize, 1), program.groups.items.len);
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
    try testing.expectEqual(@as(usize, 1), program.groups.items.len);
    try testing.expectEqual(@as(usize, 2), first_row.values.len);
    try testing.expect(first_row.values[0].value == .float);
    try testing.expect(math.isNan(first_row.values[0].value.float));
    try testing.expectEqual(@as(i64, 1), first_row.values[1].value.integer);

    const second_row = try program.execute(testing.allocator, &second_nan_event, nextTimestamp(&clock)) orelse return testing.expect(false);
    defer second_row.deinit();
    try testing.expectEqual(@as(usize, 1), program.groups.items.len);
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
    try testing.expectEqual(@as(usize, 1), program.groups.items.len);

    const maybe_row_b = try program.execute(testing.allocator, &event_b, nextTimestamp(&clock));
    try testing.expect(maybe_row_b == null);
    try testing.expectEqual(@as(usize, 2), program.groups.items.len);

    const maybe_row_c = try program.execute(testing.allocator, &event_c, nextTimestamp(&clock));
    try testing.expect(maybe_row_c == null);
    try testing.expectEqual(@as(usize, 2), program.groups.items.len);
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
    try testing.expectEqual(@as(usize, 0), program.groups.items.len);
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
    try testing.expectEqual(@as(usize, 1), program.groups.items.len);

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
