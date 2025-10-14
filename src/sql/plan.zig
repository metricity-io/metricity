const ast = @import("ast.zig");

/// Describes the full execution pipeline used by the streaming SQL runtime.
pub const PhysicalPlan = struct {
    filter: ?FilterStage = null,
    project: ProjectStage,
    group_aggregate: GroupAggregateStage,
    having: ?HavingStage = null,
    window: ?WindowStage = null,
    route: ?RouteStage = null,
};

/// Optional WHERE predicate compiled from the original statement.
pub const FilterStage = struct {
    predicate: *const ast.Expression,
};

/// Project stage prepares group keys and aggregate-ready projections.
pub const ProjectStage = struct {
    projections: []const Projection,
    group_columns: []const GroupColumn,
};

/// Aggregation stage metadata (functions and their arguments).
pub const GroupAggregateStage = struct {
    aggregates: []const AggregateSpec,
};

/// HAVING predicate evaluated after aggregates are updated.
pub const HavingStage = struct {
    predicate: *const ast.Expression,
};

/// Placeholder for future event/processing-time window semantics.
pub const WindowStage = struct {
    enabled: bool = false,
};

/// Optional routing information used by downstream distribution.
pub const RouteStage = struct {
    expression: []const u8,
};

/// Supported aggregate functions.
pub const AggregateFunction = enum {
    count,
    sum,
    avg,
    min,
    max,
};

/// Aggregate call argument: either `*` or an expression.
pub const AggregateArgument = union(enum) {
    star,
    expression: *const ast.Expression,
};

/// Compile-time specification of an aggregate call.
pub const AggregateSpec = struct {
    function: AggregateFunction,
    argument: AggregateArgument,
    call: *const ast.FunctionCall,
};

/// GROUP BY column specification.
pub const GroupColumn = struct {
    column: ast.ColumnRef,
};

/// Projection descriptor used when materialising rows.
pub const Projection = union(enum) {
    group: GroupProjection,
    aggregate: AggregateProjection,
};

pub const GroupProjection = struct {
    label: []const u8,
    column_index: usize,
};

pub const AggregateProjection = struct {
    label: []const u8,
    aggregate_index: usize,
};
