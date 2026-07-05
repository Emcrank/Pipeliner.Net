# Pipeliner.Net Enterprise Feature Roadmap

This document captures realistic, high-value feature ideas for expanding Pipeliner.Net into a more robust enterprise-ready pipeline library while preserving its current strengths: lightweight execution, strongly typed composition, async-first steps, streaming support, policies, and OpenTelemetry-friendly diagnostics.

## Current Architecture Summary

The current library already includes:

- Typed fluent composition via `Pipeline.For<TInput>()`.
- Request/response execution via `OperationPipeline<TParam, TResult>`.
- Stream execution via `Pipeline.StreamFor<TInput>()`.
- Synchronous and asynchronous steps.
- Step-level and pipeline-level execution policies.
- Binary branching via `Branch`.
- Parallel branch execution via `Fork` and `Merge`.
- Parallel projection via `ThenParallel`.
- Channel-backed stream backpressure.
- Batch execution APIs.
- OpenTelemetry-style `ActivitySource` and `Meter` instrumentation.

The main architectural limitation is that `PipelineBuilder<TInput, TCurrent>` currently stores execution as a composed delegate:

```csharp
Func<TInput, CancellationToken, ValueTask<TCurrent>>
```

That keeps execution small and efficient, but the pipeline structure is mostly lost after composition. This makes visualization, dry-runs, routing inspection, checkpointing, and advanced diagnostics harder.

The most strategic next step is to preserve lightweight pipeline metadata alongside the executable delegate chain.

## Recommended Priority Order

1. Add named steps and an internal `PipelineDefinition` graph metadata model.
2. Expand `StepExecutionOptions` with step names, max concurrency, and `System.Threading.RateLimiting` support.
3. Add stream batching and windowing APIs.
4. Add saga-style compensation.
5. Add visualization and dry-run support on top of metadata.
6. Add durable execution and broker integrations as optional packages.

## 1. Advanced Execution and Routing Control

### Dynamic Routing / Conditional Branching

The library already has `Branch`, but it is binary and both paths must converge to the same output type. A next-level feature would be named multi-route branching based on runtime data.

Suggested API:

```csharp
var pipeline = Pipeline
    .For<Payment>()
    .RouteBy(
        "PaymentMethodRouter",
        payment => payment.Method,
        routes => routes
            .When(PaymentMethod.Card, p => ChargeCardAsync(p))
            .When(PaymentMethod.BankTransfer, p => StartBankTransferAsync(p))
            .When(PaymentMethod.PayPal, p => ChargePayPalAsync(p))
            .Default(p => RejectUnsupportedAsync(p)))
    .Build();
```

Initial scope should stay simple:

- One input type.
- One output type.
- Multiple named route delegates.
- Optional default route.
- Route metadata captured in the pipeline definition.

Later versions could support graph-based routing where one step can emit to multiple named downstream steps.

### Per-Step Parallelism and Concurrency Limits

`ThenParallel` already supports max degree of parallelism for sequence projection. Enterprise users also need concurrency controls around individual steps, especially when calling databases, APIs, message brokers, or SaaS systems.

Suggested `StepExecutionOptions` expansion:

```csharp
public sealed record StepExecutionOptions(
    IPipelineExecutionPolicy? Policy = null,
    int? MaxConcurrency = null,
    RateLimiter? RateLimiter = null,
    string? Name = null);
```

Suggested API:

```csharp
var pipeline = Pipeline
    .For<IReadOnlyList<Order>>()
    .ThenParallel<Order, EnrichedOrder>(
        EnrichOrderAsync,
        ParallelStepOptions.Create(maxDegreeOfParallelism: 8))
    .ThenAsync(
        "SendToErp",
        SendToErpAsync,
        StepExecutionOptions.Create(
            maxConcurrency: 4,
            rateLimiter: new TokenBucketRateLimiter(
                new TokenBucketRateLimiterOptions
                {
                    TokenLimit = 100,
                    TokensPerPeriod = 100,
                    ReplenishmentPeriod = TimeSpan.FromMinutes(1),
                    QueueLimit = 500,
                    AutoReplenishment = true
                })))
    .Build();
```

Implementation approach:

- Use `SemaphoreSlim` for per-step max concurrency.
- Use `RateLimiter.AcquireAsync(...)` for rate-limited execution.
- Keep existing `IPipelineExecutionPolicy` as the resilience wrapper.
- Apply wrappers in a predictable order: concurrency, rate limit, policy, user step.

### Batching and Windowing

Batching fits best in `StreamPipelineBuilder`, because stream pipelines already use channels and backpressure.

Suggested API:

```csharp
var pipeline = Pipeline
    .StreamFor<OrderCreated>()
    .Batch(size: 100, maxDelay: TimeSpan.FromSeconds(5))
    .ThenAsync<IReadOnlyList<OrderCreated>, ImportResult>(
        ImportBatchAsync)
    .Build("Order import stream");

await foreach (var result in pipeline.RunStreamAsync(events, cancellationToken))
{
    Console.WriteLine(result.ImportedCount);
}
```

Implementation approach:

- Add `Batch<TCurrent>` returning `StreamPipelineBuilder<TInput, IReadOnlyList<TCurrent>>`.
- Buffer until count is reached or a timer fires.
- Flush partial batches on source completion.
- Respect cancellation.
- Preserve backpressure behavior.

Future extensions:

- `Window(TimeSpan duration)`.
- `SlidingWindow(TimeSpan duration, TimeSpan advanceBy)`.
- `GroupByWindow<TKey>(...)`.

## 2. State, Resilience and Recovery

### Stateful Pipelines

Avoid storing mutable state on the pipeline instance. Instead, provide per-run state.

Suggested API:

```csharp
var pipeline = Pipeline
    .For<Order>()
    .WithState(() => new OrderProcessingState())
    .ThenAsync((order, state, ct) =>
    {
        state.Attempts++;
        return ValidateAsync(order, ct);
    })
    .ThenAsync((validated, state, ct) =>
    {
        state.ValidatedAt = DateTimeOffset.UtcNow;
        return EnrichAsync(validated, ct);
    })
    .Build();
```

Implementation approach:

- Create one state object per `Run` or `RunAsync`.
- Pass state through a `PipelineRunContext<TState>`.
- Do not store run state on the built pipeline.
- Document that shared state must be synchronized by the user if intentionally shared.

### Transaction / Saga Support

Full distributed transactions would be too heavy for the core library. Saga-style compensation is a better fit.

Suggested API:

```csharp
var pipeline = Pipeline
    .For<CreateAccountCommand>()
    .ThenSaga(
        "CreateCustomer",
        execute: CreateCustomerAsync,
        compensate: async (customer, ct) =>
        {
            await DeleteCustomerAsync(customer.Id, ct);
        })
    .ThenSaga(
        "CreateSubscription",
        execute: CreateSubscriptionAsync,
        compensate: async (subscription, ct) =>
        {
            await CancelSubscriptionAsync(subscription.Id, ct);
        })
    .Build("Account signup");

await pipeline.RunAsync(command);
```

Implementation approach:

- Add a `SagaContext` per pipeline run.
- Each successful compensatable step registers a compensation callback.
- On failure, execute compensations in reverse order.
- Allow compensation failure strategies:
  - throw aggregate exception,
  - log and continue,
  - stop on first compensation failure.

### Checkpointing and Pause / Resume

Checkpointing is valuable but should come after metadata support. It requires stable step IDs and a resumable execution model.

Suggested API:

```csharp
var pipeline = Pipeline
    .For<OrderImport>()
    .Then("Validate", ValidateAsync)
    .Checkpoint("after-validation")
    .Then("Persist", PersistAsync)
    .WithCheckpointStore(new SqlCheckpointStore(connectionString))
    .Build();

await pipeline.RunAsync(import);

await pipeline.ResumeAsync(runId);
```

Required pieces:

- Stable step IDs.
- Serializable run context.
- Serializable intermediate payloads.
- Checkpoint store abstraction.
- Replay/resume semantics.
- Idempotency guidance.

This should likely live in an optional package such as `Pipeliner.Net.Durable`.

## 3. Developer Experience and Diagnostics

### Pipeline Visualization

Visualization depends on retaining pipeline metadata. Once a `PipelineDefinition` exists, export formats become straightforward.

Suggested API:

```csharp
var pipeline = Pipeline
    .For<Order>()
    .Then("Validate", ValidateOrder)
    .ThenAsync("Price", PriceOrderAsync)
    .Branch(
        "RouteByRisk",
        order => order.RiskScore > 80,
        high => high with { ReviewRequired = true },
        low => low)
    .Build("Order workflow");

string mermaid = pipeline.Describe().ToMermaid();
string dot = pipeline.Describe().ToDot();
string json = pipeline.Describe().ToJson();
```

Internal model:

```csharp
public sealed record PipelineDefinition(
    string Id,
    string Name,
    IReadOnlyList<PipelineNode> Nodes,
    IReadOnlyList<PipelineEdge> Edges);

public sealed record PipelineNode(
    string Id,
    string Name,
    Type InputType,
    Type OutputType,
    PipelineNodeKind Kind);

public sealed record PipelineEdge(
    string From,
    string To,
    string? Label = null);
```

### Dry-Run Mode

Dry-run support should validate configuration and flow without executing real side effects.

Suggested API:

```csharp
var report = pipeline.DryRun(new Order())
    .ValidateTypes()
    .ValidateRoutes()
    .ValidateRequiredNames()
    .ToReport();
```

Optional simulation interface:

```csharp
public interface IDryRunnableStep<TInput, TOutput>
{
    ValueTask<TOutput> SimulateAsync(TInput input, CancellationToken cancellationToken);
}
```

Behavior:

- If a step supports dry-run, call `SimulateAsync`.
- If it does not, report the step as not simulated.
- Do not execute normal delegates during dry-run unless explicitly allowed.

### Step-by-Step Tracing

The library already emits spans and metrics. A richer observer model would allow profiling, UI timelines, payload-size tracking, and diagnostics reports.

Suggested API:

```csharp
var result = await pipeline.RunAsync(
    input,
    PipelineRunOptions.Trace(trace =>
    {
        trace.IncludePayloadSizes = true;
        trace.IncludeStepInputs = false;
        trace.IncludeStepOutputs = false;
    }));

foreach (var step in result.Trace.Steps)
{
    Console.WriteLine($"{step.Name}: {step.Duration}");
}
```

Observer extension point:

```csharp
public interface IPipelineObserver
{
    ValueTask OnStepStartedAsync(StepStarted started, CancellationToken ct);
    ValueTask OnStepCompletedAsync(StepCompleted completed, CancellationToken ct);
    ValueTask OnStepFailedAsync(StepFailed failed, CancellationToken ct);
}
```

This keeps diagnostics extensible without bloating the core execution path.

## 4. Ecosystem Integration

### Durable Execution and Message Brokers

Broker and storage integrations should be optional packages rather than part of the core library.

Possible packages:

- `Pipeliner.Net.RabbitMQ`
- `Pipeliner.Net.AzureServiceBus`
- `Pipeliner.Net.SqlServer`
- `Pipeliner.Net.Postgres`
- `Pipeliner.Net.Durable`

Suggested future API:

```csharp
await Pipeline
    .StreamFor<OrderCreated>()
    .FromAzureServiceBus("orders-created")
    .ThenAsync(ProcessOrderAsync)
    .ToAzureServiceBus("orders-processed")
    .RunWorkerAsync(cancellationToken);
```

This should build on:

- stream pipelines,
- backpressure,
- batching,
- checkpointing,
- tracing,
- retry and rate-limit policies.

### System.Threading.RateLimiting Integration

Native .NET rate limiting is a strong fit for enterprise usage and should be integrated directly into `StepExecutionOptions`.

Suggested convenience API:

```csharp
var pipeline = Pipeline
    .For<ApiRequest>()
    .ThenAsync(
        "CallExternalApi",
        CallExternalApiAsync,
        StepExecutionOptions.RateLimited(
            new FixedWindowRateLimiter(
                new FixedWindowRateLimiterOptions
                {
                    PermitLimit = 50,
                    Window = TimeSpan.FromSeconds(1),
                    QueueLimit = 200
                })))
    .Build();
```

Implementation notes:

- Add a package reference only if targeting requires it.
- Ensure leases are disposed.
- Throw a clear exception when the limiter rejects execution.
- Consider exposing rejection handling options later.

## Top 4 Most Viable Features

### 1. Pipeline Metadata and Visualization

This is the best foundational feature. It enables graph export, route inspection, dry-run validation, richer diagnostics, and future durable execution.

Minimal user-facing API:

```csharp
var pipeline = Pipeline
    .For<Order>()
    .Then("Validate", ValidateOrder)
    .ThenAsync("Price", PriceOrderAsync)
    .Build("Order workflow");

var definition = pipeline.Describe();

Console.WriteLine(definition.ToMermaid());
```

### 2. Per-Step Concurrency and Rate Limiting

This directly solves enterprise integration problems where downstream systems must be protected.

Minimal user-facing API:

```csharp
var pipeline = Pipeline
    .For<Order>()
    .ThenAsync(
        "EnrichFromCrm",
        EnrichFromCrmAsync,
        StepExecutionOptions.Create(maxConcurrency: 8))
    .ThenAsync(
        "SendToBilling",
        SendToBillingAsync,
        StepExecutionOptions.RateLimited(billingRateLimiter))
    .Build();
```

### 3. Stream Batching and Windowing

This complements the existing stream and backpressure model and opens up ETL, ingestion, and integration scenarios.

Minimal user-facing API:

```csharp
var pipeline = Pipeline
    .StreamFor<EventEnvelope>()
    .Batch(size: 500, maxDelay: TimeSpan.FromSeconds(10))
    .ThenAsync<PersistResult>(PersistBatchAsync)
    .Build("Event ingestion");
```

### 4. Saga Compensation

This gives users a practical recovery model for multi-step workflows without turning the library into a heavyweight workflow engine.

Minimal user-facing API:

```csharp
var pipeline = Pipeline
    .For<CreateOrderCommand>()
    .ThenSaga(
        "ReserveInventory",
        ReserveInventoryAsync,
        compensation: (reservation, ct) => ReleaseInventoryAsync(reservation.Id, ct))
    .ThenSaga(
        "CapturePayment",
        CapturePaymentAsync,
        compensation: (payment, ct) => RefundPaymentAsync(payment.Id, ct))
    .Build("Create order");
```

## Strategic Recommendation

The best next move is to introduce an internal pipeline graph model while keeping the current delegate-chain execution path. This preserves the library's lightweight feel but gives it the enterprise capabilities it currently lacks: inspectability, documentation, validation, route clarity, diagnostics, and a path toward durability.

After that, per-step concurrency/rate limits and stream batching are the highest-return runtime features. Saga compensation should follow once the metadata and execution context model are in place.
