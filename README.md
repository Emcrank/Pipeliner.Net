# Pipeliner.Net <img src="Pipeliner.Net/Icon.png" alt="Pipeliner.Net Icon" width="40" />



[![Continous Integration](https://github.com/Emcrank/Pipeliner.Net/actions/workflows/ci.yml/badge.svg)](https://github.com/Emcrank/Pipeliner.Net/actions/workflows/ci.yml)
[![Release](https://github.com/Emcrank/Pipeliner.Net/actions/workflows/release.yml/badge.svg)](https://github.com/Emcrank/Pipeliner.Net/actions/workflows/release.yml)

A high-performance, typed pipeline library for building reusable request workflows with a fluent API.

## Why use Pipeliner.Net?

Pipeliner.Net is useful when you need to:

- compose multiple business steps into one reusable workflow,
- keep request flow strongly typed from input to output,
- run synchronous and asynchronous operations in the same pipeline,
- add resilience policies (like retries) around execution,
- process data in batches,
- branch, fork, and merge workflow paths.

Typical use cases:

- API request processing pipelines,
- ETL and transformation workflows,
- command and validation orchestration,
- integration workflows that call multiple external systems,
- background job processing with explicit, testable steps.

## Installation

```bash
dotnet add package Pipeliner.Net
```

## Quick start

```csharp
var pipeline = Pipeline
    .For<string>()
    .Then<int>(value => int.Parse(value))
    .Branch(
        value => value >= 0,
        value => value,
        _ => 0)
    .Then<int>(value => value * 2)
    .ThenAsync<int>(async (value, cancellationToken) =>
    {
        await Task.Delay(25, cancellationToken);
        return value + 10;
    })
    .Build("Quick start workflow");

var result = await pipeline.RunAsync("50");
Console.WriteLine(result);
// 110
```

## Core concepts

### OperationPipeline<TParam, TResult>

`OperationPipeline<TParam, TResult>` is the runtime pipeline type produced by the builder.

Use it for execution:

- `Run(...)` and `RunAsync(...)`
- `RunBatchAsync(...)`

### Pipeline.For<TInput>() builder

`Pipeline.For<TInput>()` is the entry point for pipeline composition and produces an `OperationPipeline` via `Build()`.

Use it for all pipeline definitions in application code.

## Fluent builder examples

### 1) Typed synchronous + asynchronous chain

```csharp
var pipeline = Pipeline
    .For<string>()
    .Then<int>(Convert.ToInt32)
    .ThenAsync<int>(async (value, cancellationToken) =>
    {
        await Task.Delay(25, cancellationToken);
        return value + 5;
    })
    .Build("Parse and increment");

var result = await pipeline.RunAsync("10");
// 15
```

### 2) Factory-based step registration

```csharp
public sealed class AddTaxStep : IPipelineStep<decimal, decimal>
{
    public ValueTask<decimal> ExecuteAsync(decimal input, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(input * 1.2m);
}

var pipeline = Pipeline
    .For<decimal>()
    .Then<AddTaxStep, decimal>(() => new AddTaxStep())
    .Build();

var total = pipeline.Run(100m);
// 120.0
```

### 3) Step-level retry policy

```csharp
var pipeline = Pipeline
    .For<int>()
    .ThenAsync(
        async (value, cancellationToken) =>
        {
            await Task.Delay(5, cancellationToken);
            return value + 1;
        },
        StepExecutionOptions.WithPolicy(new RetryExecutionPolicy(3)))
    .Build();
```

### 4) Step-level concurrency and rate limiting

```csharp
using System.Threading.RateLimiting;

var limiter = new TokenBucketRateLimiter(
    new TokenBucketRateLimiterOptions
    {
        TokenLimit = 100,
        TokensPerPeriod = 100,
        ReplenishmentPeriod = TimeSpan.FromMinutes(1),
        QueueLimit = 250,
        AutoReplenishment = true
    });

var pipeline = Pipeline
    .For<Order>()
    .ThenAsync(
        "Send to ERP",
        SendToErpAsync,
        StepExecutionOptions.Create(
            maxConcurrency: 4,
            rateLimiter: limiter))
    .Build();
```

A rejected rate-limit lease throws `PipelineRateLimitRejectedException`.
### 5) Saga compensation

```csharp
var pipeline = Pipeline
    .For<CreateOrderCommand>()
    .ThenSaga(
        "Reserve inventory",
        ReserveInventoryAsync,
        (reservation, cancellationToken) => ReleaseInventoryAsync(reservation.Id, cancellationToken))
    .ThenSaga(
        "Capture payment",
        CapturePaymentAsync,
        (payment, cancellationToken) => RefundPaymentAsync(payment.Id, cancellationToken))
    .Build("Create order");
```

If a later step fails, completed saga compensations run in reverse order. If compensation itself fails, `PipelineSagaCompensationException` exposes the original pipeline exception and all compensation failures.
### 6) Per-run state

```csharp
public sealed class OrderState
{
    public int Attempts { get; set; }
    public DateTimeOffset? ValidatedAt { get; set; }
}

var pipeline = Pipeline
    .For<Order>()
    .WithState(() => new OrderState())
    .ThenAsync("Validate", async (order, state, cancellationToken) =>
    {
        state.Attempts++;
        state.ValidatedAt = DateTimeOffset.UtcNow;
        return await ValidateAsync(order, cancellationToken);
    })
    .Then("Apply state", (order, state) => order with { Attempts = state.Attempts })
    .Build("Stateful order workflow");
```

State is created once per pipeline run, so concurrent executions do not share mutable state.
### 7) Pipeline-level execution policy

```csharp
var pipeline = Pipeline
    .For<int>()
    .ThenAsync<int>((value, _) => ValueTask.FromResult(value + 1))
    .WithPolicy(new RetryExecutionPolicy(2))
    .Build();
```

### 8) Dynamic routing

```csharp
var pipeline = Pipeline
    .For<Payment>()
    .RouteBy<PaymentMethod, PaymentResult>(
        "Payment method route",
        payment => payment.Method,
        routes => routes
            .When(PaymentMethod.Card, ChargeCard)
            .WhenAsync(PaymentMethod.BankTransfer, StartBankTransferAsync)
            .Default(payment => PaymentResult.Rejected(payment.Id)))
    .Build();
```

If no route matches and no default route is configured, `PipelineRouteNotFoundException` is thrown.
### 9) Branch and branch async

```csharp
var pipeline = Pipeline
    .For<int>()
    .Branch(
        value => value >= 0,
        value => value,
        _ => 0)
    .BranchAsync(
        value => value > 100,
        (value, _) => ValueTask.FromResult($"large:{value}"),
        (value, _) => ValueTask.FromResult($"small:{value}"))
    .Build();

var label = await pipeline.RunAsync(150);
// large:150
```

### 10) Fork + merge (custom reducer)

```csharp
var pipeline = Pipeline
    .For<decimal>()
    .Fork<decimal>(
        (amount, _) => ValueTask.FromResult(amount + 5m),
        (amount, _) => ValueTask.FromResult(amount * 1.08m),
        (amount, _) => ValueTask.FromResult(amount - 3m))
    .Merge<decimal, decimal>((results, _) => ValueTask.FromResult(results.Sum()), MergeStepOptions.CustomReducer())
    .Build("Price workflow");

var finalPrice = await pipeline.RunAsync(120m);
Console.WriteLine(finalPrice);
// 371.6
```

### 11) Built-in merge strategy: throw on any failure

```csharp
var pipeline = Pipeline
    .For<int>()
    .Fork<int>(
        (value, _) => ValueTask.FromResult(value + 1),
        (_, _) => ValueTask.FromException<int>(new InvalidOperationException("branch failed")),
        (value, _) => ValueTask.FromResult(value + 3))
    .Merge<int, IReadOnlyList<int>>(
        (results, _) => ValueTask.FromResult<IReadOnlyList<int>>(results),
        MergeStepOptions.ThrowOnAnyFailure())
    .Build();

await Assert.ThrowsAsync<AggregateException>(() => pipeline.RunAsync(10));
```

### 12) Built-in merge strategy: ignore failures

```csharp
var pipeline = Pipeline
    .For<int>()
    .Fork<int>(
        (value, _) => ValueTask.FromResult(value + 1),
        (_, _) => ValueTask.FromException<int>(new InvalidOperationException("branch failed")),
        (value, _) => ValueTask.FromResult(value + 3))
    .Merge<int, IReadOnlyList<int>>(
        (results, _) => ValueTask.FromResult<IReadOnlyList<int>>(results),
        MergeStepOptions.IgnoreFailures())
    .Build();

var results = await pipeline.RunAsync(10);
// [11, 13]
```

### 13) Built-in merge strategy: take first

```csharp
var pipeline = Pipeline
    .For<int>()
    .Fork<int>(
        (value, _) => ValueTask.FromResult(value + 1),
        (value, _) => ValueTask.FromResult(value + 2))
    .Merge<int, int>(
        (results, _) => ValueTask.FromResult(results[0]),
        MergeStepOptions.TakeFirst())
    .Build();

var first = await pipeline.RunAsync(10);
// 11
```

### 14) Parallel projection

```csharp
var pipeline = Pipeline
    .For<int[]>()
    .ThenParallel<int, int>(
        (value, _) => ValueTask.FromResult(value * value),
        ParallelStepOptions.Create(4))
    .Build();

var squares = await pipeline.RunAsync([1, 2, 3, 4]);
// [1, 4, 9, 16]
```

## Batch execution

### Batch from memory

```csharp
var pipeline = Pipeline
    .For<string>()
    .Then<int>(Convert.ToInt32)
    .Then<int>(value => value + 1)
    .Build();

var results = await pipeline.RunBatchAsync(new[] { "1", "2", "3" });
// [2, 3, 4]
```

### Batch from IAsyncEnumerable<T>

```csharp
static async IAsyncEnumerable<string> GetInputsAsync()
{
    yield return "10";
    await Task.Yield();
    yield return "20";
}

var pipeline = Pipeline
    .For<string>()
    .Then<int>(Convert.ToInt32)
    .Then<int>(value => value + 5)
    .Build();

await foreach (var item in pipeline.RunBatchAsync(GetInputsAsync()))
{
    Console.WriteLine(item);
}
```

## Stream execution with backpressure

### Stream builder quick start

```csharp
var streamPipeline = Pipeline
    .StreamFor<string>()
    .Then<int>(int.Parse)
    .ThenAsync<int>(async (value, cancellationToken) =>
    {
        await Task.Delay(10, cancellationToken);
        return value + 1;
    })
    .Build("Stream parse and increment");

await foreach (var item in streamPipeline.RunStreamAsync(GetInputsAsync()))
{
    Console.WriteLine(item);
}
```

### Configure bounded channel backpressure

```csharp
var streamPipeline = Pipeline
    .StreamFor<int>()
    .WithBackpressure(BackpressureOptions.Create(256, BackpressureMode.Wait))
    .Then<int>(value => value * 2)
    .Build();
```

### Batch and window stream items

```csharp
var batchedPipeline = Pipeline
    .StreamFor<OrderCreated>()
    .Batch(size: 100, maxDelay: TimeSpan.FromSeconds(5))
    .ThenAsync<ImportResult>(ImportBatchAsync)
    .Build("Order import batches");

await foreach (var result in batchedPipeline.RunStreamAsync(events, cancellationToken))
{
    Console.WriteLine(result.ImportedCount);
}
```

```csharp
var windowedPipeline = Pipeline
    .StreamFor<MetricPoint>()
    .Window(TimeSpan.FromSeconds(10))
    .Then(window => window.Average(point => point.Value))
    .Build("Metric windows");
```
Available backpressure modes:

- `BackpressureMode.Wait`
- `BackpressureMode.DropNewest`
- `BackpressureMode.DropOldest`
- `BackpressureMode.DropWrite`

## MergeReducers helper

Use `MergeReducers` directly when you need custom aggregation over detailed branch outcomes (`ForkResult<T>`):

```csharp
var forkPipeline = Pipeline
    .For<int>()
    .Fork<int>(
        (value, _) => ValueTask.FromResult(value + 1),
        (_, _) => ValueTask.FromException<int>(new InvalidOperationException("branch failed")),
        (value, _) => ValueTask.FromResult(value + 3))
    .Build();

var forkExecution = await forkPipeline.RunAsync(10);

var reduced = await MergeReducers.ReduceAsync(
    forkExecution.BranchResults,
    0,
    (acc, value, _) => ValueTask.FromResult(acc + value));

Console.WriteLine(reduced);
// 24
```

## Dependency injection friendly operations

```csharp
var pipeline = Pipeline
    .For<int>()
    .Then<MyServiceStep, int>(() => new MyServiceStep(serviceProvider.GetRequiredService<MyService>()))
    .Build();

public sealed class MyServiceStep(MyService service) : IPipelineStep<int, int>
{
    public ValueTask<int> ExecuteAsync(int input, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(service.Transform(input));
}
```


## Pipeline descriptions and visualization

Built pipelines expose structural metadata through `Describe()`. Named overloads make exported graphs readable for documentation, diagnostics, or UI rendering.

```csharp
var pipeline = Pipeline
    .For<Order>()
    .Then("Validate", ValidateOrder)
    .ThenAsync("Price", PriceOrderAsync)
    .Branch(
        "Route by risk",
        order => order.RiskScore > 80,
        high => high with { ReviewRequired = true },
        low => low)
    .Build("Order workflow");

var definition = pipeline.Describe();

string mermaid = definition.ToMermaid();
string dot = definition.ToDot();
string json = definition.ToJson();
```

`PipelineDefinition` contains nodes, edges, node kinds, and input/output types. Stream pipelines expose the same `Describe()` API.

## Step tracing

Use `RunWithTrace(...)` or `RunWithTraceAsync(...)` to execute a pipeline and capture per-step timing metadata for fluent steps.

```csharp
var run = await pipeline.RunWithTraceAsync(input, cancellationToken);

Console.WriteLine(run.Result);

foreach (var step in run.Trace.Steps)
{
    Console.WriteLine($"{step.Name}: {step.Duration.TotalMilliseconds}ms");
}
```

Trace entries include the step name, kind, input/output types, duration, success flag, and exception type when captured around a failing step.
## Dry-run validation

Use `DryRun()` to validate the captured pipeline structure without executing any step delegates or side effects.

```csharp
var report = pipeline.DryRun();

if (!report.IsValid)
{
    foreach (var issue in report.Issues)
    {
        Console.WriteLine($"{issue.Severity}: {issue.Code} - {issue.Message}");
    }
}
```

Dry-run validation checks graph consistency, missing edge endpoints, duplicate node IDs, and unreachable nodes.
## Observability

`OperationPipeline` emits:

- `ActivitySource` spans using source name `Pipeliner.Net`,
- `Meter` metrics:
  - `pipeliner.pipeline.runs`
  - `pipeliner.pipeline.failures`
  - `pipeliner.pipeline.duration.ms`
  - `pipeliner.pipeline.operation.duration.ms`

This integrates cleanly with OpenTelemetry collectors and exporters.

## Error handling model

- Per-operation exception handling via `onExceptionHandler`.
- Unhandled exceptions bubble to caller.
- Retry and similar behavior can be applied via `IPipelineExecutionPolicy`.

## Testing guidance

Pipelines are easy to unit test because:

- each step is just a delegate or `IPipelineStep`,
- full flow can be executed in-memory,
- branch/fork/merge behavior is deterministic and explicit.

## Roadmap alignment

Current API includes:

- async-first delegates with `ValueTask`,
- typed fluent composition,
- batch APIs,
- streaming APIs with channel-backed backpressure,
- policy hooks,
- branch/fork/merge capabilities,
- built-in merge conflict strategies and reducers,
- instrumentation support.

Future phases can expand higher-level integration helpers and additional execution policies without breaking request-response usage.
