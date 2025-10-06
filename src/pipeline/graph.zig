const std = @import("std");
const config_mod = @import("../config/mod.zig");

pub const BuildError = error{CycleDetected};

pub const NodeKind = enum { source, transform, sink };

pub const ComponentRef = union(NodeKind) {
    source: usize,
    transform: usize,
    sink: usize,
};

pub const Node = struct {
    id: []const u8,
    kind: NodeKind,
    component: ComponentRef,
};

pub const Connections = struct {
    incoming: []usize,
    outgoing: []usize,
};

pub const Graph = struct {
    allocator: std.mem.Allocator,
    nodes: []Node,
    connections: []Connections,
    topo_order: []usize,

    pub fn deinit(self: *Graph) void {
        for (self.connections) |conn| {
            self.allocator.free(conn.incoming);
            self.allocator.free(conn.outgoing);
        }
        self.allocator.free(self.connections);
        self.allocator.free(self.nodes);
        self.allocator.free(self.topo_order);
        self.* = undefined;
    }
};

pub fn build(allocator: std.mem.Allocator, cfg: *const config_mod.PipelineConfig) (std.mem.Allocator.Error || BuildError)!Graph {
    const total_nodes = cfg.sources.len + cfg.transforms.len + cfg.sinks.len;
    var nodes = try allocator.alloc(Node, total_nodes);
    errdefer allocator.free(nodes);

    var connections = try allocator.alloc(Connections, total_nodes);
    errdefer allocator.free(connections);

    var builders = try allocator.alloc(NodeBuilder, total_nodes);
    errdefer {
        for (builders) |*builder| {
            builder.incoming.deinit(allocator);
            builder.outgoing.deinit(allocator);
        }
        allocator.free(builders);
    }

    var id_to_index = std.StringHashMap(usize).init(allocator);
    defer id_to_index.deinit();

    var next_index: usize = 0;

    for (cfg.sources, 0..) |src, idx| {
        const node_index = next_index;
        next_index += 1;
        nodes[node_index] = .{
            .id = src.id,
            .kind = .source,
            .component = ComponentRef{ .source = idx },
        };
        id_to_index.put(src.id, node_index) catch |err| switch (err) {
            error.OutOfMemory => return err,
        };
        builders[node_index] = .{};
    }

    for (cfg.transforms, 0..) |transform, idx| {
        const node_index = next_index;
        next_index += 1;
        nodes[node_index] = .{
            .id = transform.id(),
            .kind = .transform,
            .component = ComponentRef{ .transform = idx },
        };
        id_to_index.put(transform.id(), node_index) catch |err| switch (err) {
            error.OutOfMemory => return err,
        };
        builders[node_index] = .{};
    }

    for (cfg.sinks, 0..) |sink, idx| {
        const node_index = next_index;
        next_index += 1;
        nodes[node_index] = .{
            .id = sink.id(),
            .kind = .sink,
            .component = ComponentRef{ .sink = idx },
        };
        id_to_index.put(sink.id(), node_index) catch |err| switch (err) {
            error.OutOfMemory => return err,
        };
        builders[node_index] = .{};
    }

    // Build edges based on inputs of transforms and sinks.
    for (cfg.transforms) |transform| {
        const target_index = id_to_index.get(transform.id()) orelse unreachable;
        for (transform.inputs()) |input_id| {
            const source_index = id_to_index.get(input_id) orelse unreachable;
            try builders[source_index].outgoing.append(allocator, target_index);
            try builders[target_index].incoming.append(allocator, source_index);
        }
    }

    for (cfg.sinks) |sink| {
        const target_index = id_to_index.get(sink.id()) orelse unreachable;
        for (sink.inputs()) |input_id| {
            const source_index = id_to_index.get(input_id) orelse unreachable;
            try builders[source_index].outgoing.append(allocator, target_index);
            try builders[target_index].incoming.append(allocator, source_index);
        }
    }

    // Finalize connections slices.
    for (builders, 0..) |*builder, idx| {
        const incoming_slice = try builder.incoming.toOwnedSlice(allocator);
        const outgoing_slice = try builder.outgoing.toOwnedSlice(allocator);
        connections[idx] = .{
            .incoming = incoming_slice,
            .outgoing = outgoing_slice,
        };
    }

    allocator.free(builders);

    var topo_order = try allocator.alloc(usize, total_nodes);
    errdefer allocator.free(topo_order);

    var indegree = try allocator.alloc(usize, total_nodes);
    defer allocator.free(indegree);

    for (connections, 0..) |conn, idx| {
        indegree[idx] = conn.incoming.len;
    }

    var queue = std.array_list.AlignedManaged(usize, null).init(allocator);
    defer queue.deinit();

    for (indegree, 0..) |deg, idx| {
        if (deg == 0) {
            try queue.append(idx);
        }
    }

    var processed: usize = 0;
    while (queue.pop()) |node_index| {
        topo_order[processed] = node_index;
        processed += 1;

        for (connections[node_index].outgoing) |neighbor| {
            std.debug.assert(indegree[neighbor] > 0);
            indegree[neighbor] -= 1;
            if (indegree[neighbor] == 0) {
                try queue.append(neighbor);
            }
        }
    }

    if (processed != total_nodes) {
        return BuildError.CycleDetected;
    }

    return Graph{
        .allocator = allocator,
        .nodes = nodes,
        .connections = connections,
        .topo_order = topo_order,
    };
}

const NodeBuilder = struct {
    incoming: std.ArrayListUnmanaged(usize) = .{},
    outgoing: std.ArrayListUnmanaged(usize) = .{},
};
