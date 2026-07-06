# Checkpointing and Durable Execution Design

This document defines the design direction for checkpointing, pause/resume, and durable execution in Pipeliner.Net.

The main constraint in the current architecture is important: `PipelineBuilder<TInput, TCurrent>` keeps execution as a composed delegate chain. The library also has a useful `PipelineDefinition` graph, but that graph is currently descriptive metadata, not the executable plan. Because of that, checkpoint recording can be introduced incrementally, but true resume-after-crash requires stable executable step boundaries.

## Goals

- Persist pipeline progress at explicit, named checkpoints.
- Allow a failed, paused, or interrupted run to resume from the latest valid checkpoint.
- Keep the core library lightweight.
- Avoid forcing message broker or database dependencies into the core package.
- Preserve the existing typed fluent API where possible.
- Make durable execution explicit, not an accidental side effect of normal `RunAsync`.

## Non-Goals for the First Implementation

- Transparent continuation from the middle of an arbitrary delegate.
- Distributed workflow orchestration equivalent to Temporal, Durable Functions, or Dapr Workflow.
- Durable execution for stream pipelines in the first release.
- Automatic exactly-once semantics for side effects.
- Durable execution without serializable payloads.

## Definitions

- **Checkpoint**: a persisted snapshot of a pipeline run at a named step boundary.
- **Pause**: a controlled stop after saving a checkpoint.
- **Resume**: continuing execution from a saved checkpoint.
- **Durable execution**: storing enough run state to recover after process failure or worker handoff.
- **Lease**: a time-bounded claim that allows one worker to execute a durable run.
- **Compensation**: a user-provided rollback action for a completed saga step.

## Current Architecture Impact

The current request pipeline path looks like this:

```csharp
Pipeline
    .For<TInput>()
    .Then(...)
    .ThenAsync(...)
    .Build();
```

Internally, each builder stage wraps the previous stage:

```csharp
Func<TInput, CancellationToken, ValueTask<TCurrent>>
```

`Build()` then adds one operation to `OperationPipeline<TParam, TResult>` that executes the whole composed chain.

This means:

- The library can describe steps through `PipelineDefinition`.
- The library cannot currently restart execution from step `N` because step `N` is not independently executable.
- Checkpointing can observe and persist values at boundaries.
- Durable resume needs a new internal execution plan that stores executable nodes as well as metadata nodes.

## Recommended Architecture

Implement this in phases.

### Phase 1: Checkpoint Recording

Add explicit checkpoint nodes that persist the current payload during normal execution. This gives immediate value for audit, diagnostics, and manual recovery, while avoiding premature changes to the runtime.

Suggested API:

```csharp
var pipeline = Pipeline
    .For<OrderImport>()
    .ThenAsync("Validate", ValidateAsync)
    .Checkpoint("after-validation")
    .ThenAsync("Persist", PersistAsync)
    .WithCheckpointStore(checkpointStore)
    .Build("Order import");

await pipeline.RunAsync(import, cancellationToken);
```

Expected behavior:

- `Checkpoint("after-validation")` stores the current typed value.
- Checkpoint names must be stable and unique within a pipeline.
- The checkpoint store receives pipeline id, run id, checkpoint id, payload type, payload bytes, and timestamp.
- Normal `RunAsync` behavior remains unchanged except for persistence failure handling.

Persistence failure should be configurable:

```csharp
public enum CheckpointFailureMode
{
    FailRun,
    LogAndContinue
}
```

Recommended default: `FailRun`. A checkpoint that silently fails is dangerous in durable scenarios.

### Phase 2: Executable Step Plan

Introduce an internal execution plan alongside the existing graph metadata.

Conceptual model:

```csharp
internal sealed class PipelineExecutionPlan
{
    public IReadOnlyList<PipelineExecutionNode> Nodes { get; }
}

internal sealed class PipelineExecutionNode
{
    public string Id { get; }
    public string Name { get; }
    public Type InputType { get; }
    public Type OutputType { get; }
    public PipelineNodeKind Kind { get; }
    public Func<object?, CancellationToken, ValueTask<object?>> ExecuteAsync { get; }
}
```

The existing composed delegate can stay as the fast path. Durable execution should use the executable plan path.

Builder implication:

- `ThenAsync` adds an executable node.
- `Branch`, `RouteBy`, `Fork`, and `Merge` add executable control nodes.
- `Checkpoint` adds a persistence node.
- `Build()` creates both `OperationPipeline` and `PipelineDefinition`.

This is the minimum runtime change needed for real resume.

### Phase 3: Resume from Checkpoint

Once the executable plan exists, durable execution can resume from the node after the latest checkpoint.

Suggested API:

```csharp
var pipeline = Pipeline
    .For<OrderImport>()
    .ThenAsync("Validate", ValidateAsync)
    .Checkpoint("after-validation")
    .ThenAsync("Persist", PersistAsync)
    .Checkpoint("after-persist")
    .ThenAsync("Publish", PublishAsync)
    .WithDurability(options => options
        .UseCheckpointStore(checkpointStore)
        .UseSerializer(serializer))
    .Build("Order import");

PipelineRunHandle run = await pipeline.RunDurableAsync(import, cancellationToken);

await pipeline.ResumeAsync(run.RunId, cancellationToken);
```

Resume behavior:

- Load latest checkpoint for `pipeline.Id` and `runId`.
- Deserialize payload into the checkpoint output type.
- Find the next executable node after the checkpoint node.
- Continue executing nodes until completion, failure, pause, or cancellation.
- Record run status changes.

Checkpoint compatibility rules:

- The checkpoint must belong to the same pipeline id.
- The checkpoint must reference a node id that still exists.
- The persisted payload type must match the node output type, or be handled by the configured serializer.
- Pipeline graph versioning should be introduced before schema evolution is supported.

### Phase 4: Durable Workers and Leases

Add durable worker coordination for background execution and recovery.

Suggested API:

```csharp
await pipeline.EnqueueDurableAsync(import, cancellationToken);

await pipeline.RunDurableWorkerAsync(
    new DurableWorkerOptions
    {
        WorkerId = Environment.MachineName,
        MaxConcurrentRuns = 8,
        LeaseDuration = TimeSpan.FromMinutes(2),
        PollInterval = TimeSpan.FromSeconds(5)
    },
    cancellationToken);
```

Worker behavior:

- Claim pending or expired runs with a lease.
- Execute from the latest checkpoint or initial input.
- Renew lease while executing.
- Mark runs completed, failed, paused, or abandoned.
- Release lease on graceful shutdown.

This should live outside the core package unless there is a strong reason to include it.

## Core Abstractions

### Checkpoint Store

```csharp
public interface IPipelineCheckpointStore
{
    ValueTask SaveCheckpointAsync(
        PipelineCheckpoint checkpoint,
        CancellationToken cancellationToken = default);

    ValueTask<PipelineCheckpoint?> GetLatestCheckpointAsync(
        string pipelineId,
        string runId,
        CancellationToken cancellationToken = default);

    ValueTask SaveRunStateAsync(
        PipelineRunState runState,
        CancellationToken cancellationToken = default);

    ValueTask<PipelineRunState?> GetRunStateAsync(
        string pipelineId,
        string runId,
        CancellationToken cancellationToken = default);
}
```

### Durable Run Store

For workers and leases, split run coordination from checkpoint snapshots:

```csharp
public interface IPipelineDurableRunStore : IPipelineCheckpointStore
{
    ValueTask CreateRunAsync(
        PipelineRunState runState,
        CancellationToken cancellationToken = default);

    ValueTask<PipelineRunLease?> TryAcquireLeaseAsync(
        string pipelineId,
        string workerId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    ValueTask RenewLeaseAsync(
        string pipelineId,
        string runId,
        string workerId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    ValueTask ReleaseLeaseAsync(
        string pipelineId,
        string runId,
        string workerId,
        CancellationToken cancellationToken = default);
}
```

### Checkpoint Model

```csharp
public sealed record PipelineCheckpoint(
    string PipelineId,
    string PipelineName,
    string RunId,
    string CheckpointId,
    string CheckpointName,
    string NodeId,
    string PayloadType,
    string SerializerContentType,
    byte[] Payload,
    DateTimeOffset CreatedAt);
```

### Run State Model

```csharp
public sealed record PipelineRunState(
    string PipelineId,
    string PipelineName,
    string RunId,
    PipelineRunStatus Status,
    string? CurrentNodeId,
    string? LatestCheckpointId,
    int Attempt,
    string? ErrorType,
    string? ErrorMessage,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

public enum PipelineRunStatus
{
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
    Abandoned
}
```

### Serializer

Do not hard-code JSON as the only option. Use JSON as the default serializer, but keep an abstraction.

```csharp
public interface IPipelinePayloadSerializer
{
    string ContentType { get; }

    ValueTask<byte[]> SerializeAsync<T>(
        T value,
        CancellationToken cancellationToken = default);

    ValueTask<T> DeserializeAsync<T>(
        byte[] payload,
        CancellationToken cancellationToken = default);
}
```

Default implementation:

```csharp
public sealed class JsonPipelinePayloadSerializer : IPipelinePayloadSerializer
{
    public string ContentType => "application/json";
}
```

## Package Boundary

Recommended package split:

- `Pipeliner.Net`: core checkpoint abstractions, builder APIs, in-memory store for tests, JSON serializer.
- `Pipeliner.Net.Durable`: durable worker runtime, leases, run queue abstractions.
- `Pipeliner.Net.Durable.SqlServer`: SQL Server store.
- `Pipeliner.Net.Durable.Postgres`: PostgreSQL store.
- `Pipeliner.Net.Durable.AzureStorage`: Azure Table/Blob store.
- `Pipeliner.Net.RabbitMQ`: broker source/sink integration.
- `Pipeliner.Net.AzureServiceBus`: broker source/sink integration.

Rationale:

- The core package remains lightweight.
- Users can adopt checkpoint recording without a database dependency.
- Broker-specific dependencies stay optional.
- Durable worker semantics can evolve without bloating the normal pipeline path.

## Storage Provider Recommendation

Start with two providers:

1. `InMemoryCheckpointStore` in core for tests and examples.
2. `FileCheckpointStore` or `SqliteCheckpointStore` as a local durable proof of concept.

For enterprise production support, SQL Server is likely the best first serious provider because it is common in .NET enterprise environments and supports transactional leases cleanly.

## Broker Integration Design

Durable execution and broker integration should be related but separate.

Broker packages should provide:

```csharp
await Pipeline
    .StreamFor<OrderCreated>()
    .FromAzureServiceBus("orders-created")
    .ThenAsync("Process", ProcessOrderAsync)
    .ToAzureServiceBus("orders-processed")
    .RunWorkerAsync(cancellationToken);
```

The durable runtime should provide:

```csharp
await pipeline.EnqueueDurableAsync(order, cancellationToken);
await pipeline.RunDurableWorkerAsync(workerOptions, cancellationToken);
```

Integration point:

- Broker consumers can enqueue durable runs.
- Broker producers can publish output after durable completion.
- Message acknowledgements should happen only after checkpoint/run-state persistence succeeds.

Avoid coupling the durable run store directly to RabbitMQ or Azure Service Bus. The broker should move messages; the durable store should own run recovery.

## Idempotency and Side Effects

Durable execution cannot guarantee exactly-once external side effects by itself.

Design requirements:

- Document that durable steps must be idempotent or compensatable.
- Add optional idempotency metadata later.
- Store step attempts for durable runs.
- Expose run id and step id so user code can use them as idempotency keys.

Possible future API:

```csharp
.ThenAsync(
    "CapturePayment",
    CapturePaymentAsync,
    StepExecutionOptions.Create(
        idempotencyKey: context => $"{context.RunId}:{context.StepId}"))
```

This likely requires a public `PipelineExecutionContext`, so it should not be forced into the first checkpoint implementation.

## Pause and Resume API

Recommended explicit pause model:

```csharp
public sealed class PipelinePauseException : Exception
{
    public string CheckpointName { get; }
}
```

User-facing API:

```csharp
var pipeline = Pipeline
    .For<OrderImport>()
    .ThenAsync("Validate", ValidateAsync)
    .Checkpoint("after-validation", options => options.PauseWhen(input => input.RequiresApproval))
    .ThenAsync("Persist", PersistAsync)
    .Build("Order import");
```

Alternative explicit API:

```csharp
.Pause("awaiting-approval", when: import => import.RequiresApproval)
```

Recommendation: keep `Checkpoint` and `Pause` separate. A checkpoint persists state; a pause changes run control flow.

## Minimal Implementation Sequence

1. Add checkpoint models and serializer abstractions.
2. Add `PipelineNodeKind.Checkpoint`.
3. Add `Checkpoint(...)` to `PipelineBuilder`.
4. Add `WithCheckpointStore(...)` or `WithDurability(...)` builder options.
5. Implement checkpoint recording in the current delegate chain.
6. Add unit tests for checkpoint payload, naming, failure mode, and graph output.
7. Introduce internal executable plan types.
8. Route durable execution through the executable plan.
9. Add `RunDurableAsync` and `ResumeAsync`.
10. Add in-memory durable store tests.
11. Add local durable provider proof of concept.
12. Add durable worker leases.

## Open Decisions

These decisions should be made before implementation moves beyond checkpoint recording:

- Should the first code change implement checkpoint recording only, or should it immediately include the executable plan refactor?
- Should durable APIs live in the core package initially, or in a new `Pipeliner.Net.Durable` package from the start?
- Which production store should be targeted first: SQL Server, PostgreSQL, SQLite, Azure Storage, or file system?
- Should stream pipelines be excluded from durable execution v1?
- Should checkpoint payloads require JSON serializability by default, or should the user be forced to configure a serializer?
- Should checkpoint persistence failures fail the run by default?

## Recommended Decisions

- Implement checkpoint recording first.
- Add the executable plan refactor before promising resume.
- Keep durable workers and production stores in optional packages.
- Support request/response pipelines first.
- Use JSON as the default serializer with a serializer abstraction.
- Fail the run by default when checkpoint persistence fails.
- Treat exactly-once delivery as out of scope; support idempotency keys later.

