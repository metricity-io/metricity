//! Test helpers for wiring runtime services through the generic DI container.

const std = @import("std");

pub fn RuntimeTesting(
    comptime RuntimeServices: type,
    comptime PipelineHandle: type,
    comptime PipelineVTable: type,
    comptime PipelineError: type,
    comptime PipelineConfigPtrType: type,
    comptime makeHandleFn: fn (*anyopaque, *const PipelineVTable) PipelineHandle,
) type {
    return struct {
        const Self = @This();

        pub const PipelineDouble = struct {
            pub const PollStep = union(enum) {
                value: bool,
                err: PipelineError,
            };

            start_error: ?PipelineError = null,
            poll_steps: []const PollStep = &.{},
            default_poll_result: bool = false,
            shutdown_error: ?PipelineError = null,
            start_calls: usize = 0,
            poll_calls: usize = 0,
            shutdown_calls: usize = 0,
            deinit_calls: usize = 0,
            started: bool = false,
            poll_index: usize = 0,
            already_shutdown: bool = false,

            fn nextPollStep(self: *PipelineDouble) ?PollStep {
                if (self.poll_index >= self.poll_steps.len) return null;
                const step = self.poll_steps[self.poll_index];
                self.poll_index += 1;
                return step;
            }

            fn startFn(context: *anyopaque) PipelineError!void {
                const self = asPipelineDouble(context);
                self.start_calls += 1;
                if (self.start_error) |err| {
                    return err;
                }
                self.started = true;
            }

            fn pollFn(context: *anyopaque) PipelineError!bool {
                const self = asPipelineDouble(context);
                self.poll_calls += 1;

                if (!self.started) {
                    return false;
                }

                if (self.nextPollStep()) |step| {
                    return switch (step) {
                        .value => |value| value,
                        .err => |err| err,
                    };
                }

                return self.default_poll_result;
            }

            fn shutdownFn(context: *anyopaque) PipelineError!void {
                const self = asPipelineDouble(context);
                if (!self.started or self.already_shutdown) {
                    self.started = false;
                    return;
                }

                self.already_shutdown = true;
                self.started = false;
                self.shutdown_calls += 1;
                if (self.shutdown_error) |err| return err;
            }

            fn deinitFn(context: *anyopaque) void {
                const self = asPipelineDouble(context);
                self.deinit_calls += 1;
                if (self.started and !self.already_shutdown) {
                    self.started = false;
                    self.already_shutdown = true;
                    self.shutdown_calls += 1;
                    if (self.shutdown_error == null) return;
                }
            }

            pub fn handle(self: *PipelineDouble) PipelineHandle {
                return makeHandleFn(self, &pipeline_double_vtable);
            }

            pub fn reset(self: *PipelineDouble) void {
                self.start_calls = 0;
                self.poll_calls = 0;
                self.shutdown_calls = 0;
                self.deinit_calls = 0;
                self.started = false;
                self.poll_index = 0;
                self.already_shutdown = false;
            }
        };

        pub fn servicesWithPipelineDouble(base: RuntimeServices, double: *PipelineDouble) RuntimeServices {
            active_pipeline_double = double;
            var services = base;
            services.pipelineFactory = pipelineDoubleFactory;
            return services;
        }

        pub fn clearOverride() void {
            active_pipeline_double = null;
        }

        fn asPipelineDouble(context: *anyopaque) *PipelineDouble {
            return @ptrCast(@alignCast(context));
        }

        fn pipelineDoubleFactory(
            allocator: std.mem.Allocator,
            _: PipelineConfigPtrType,
        ) PipelineError!PipelineHandle {
            _ = allocator;
            std.debug.assert(active_pipeline_double != null);
            return active_pipeline_double.?.handle();
        }

        const pipeline_double_vtable = PipelineVTable{
            .start = PipelineDouble.startFn,
            .poll_once = PipelineDouble.pollFn,
            .shutdown = PipelineDouble.shutdownFn,
            .deinit = PipelineDouble.deinitFn,
        };

        var active_pipeline_double: ?*PipelineDouble = null;
    };
}
