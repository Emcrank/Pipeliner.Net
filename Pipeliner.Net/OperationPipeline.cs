using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Pipeliner.Net;

/// <summary>
/// Represents a reusable pipeline of operations that can be executed synchronously or asynchronously.
/// </summary>
/// <typeparam name="TParam">The pipeline input type.</typeparam>
/// <typeparam name="TResult">The pipeline result type.</typeparam>
public sealed class OperationPipeline<TParam, TResult>
{
    private readonly List<IUntypedOperation> operations = [];
    private readonly AsyncLocal<RunContext?> currentRunContext = new();
    private Func<TResult?>? configuredResultFactory;
    private PipelineDefinition? definition;

    /// <summary>
    /// Initializes a new instance of <see cref="OperationPipeline{TParam,TResult}" />
    /// </summary>
    /// <param name="logger">The optional logger for pipeline diagnostics.</param>
    internal OperationPipeline(ILogger? logger = null)
    {
        Logger = logger;
    }

    /// <summary>
    /// Gets or sets the unique pipeline identifier.
    /// </summary>
    public string Id { get; internal set; } = Guid.NewGuid().ToString("D");

    /// <summary>
    /// Gets or sets the friendly pipeline name.
    /// </summary>
    public string Name { get; internal set; } = "Unnamed_Pipeline";

    /// <summary>
    /// Gets a structural description of this pipeline.
    /// </summary>
    /// <returns>The pipeline definition graph.</returns>
    public PipelineDefinition Describe() => definition ??= PipelineGraph
        .Create(typeof(TParam))
        .AddStep("Result", typeof(TParam), typeof(TResult), PipelineNodeKind.Step)
        .ToDefinition(Id, Name);

    /// <summary>
    /// Validates the pipeline definition without executing pipeline steps.
    /// </summary>
    /// <returns>A dry-run validation report.</returns>
    public PipelineDryRunReport DryRun() => PipelineDryRunReport.Validate(Describe());

    /// <summary>
    /// Gets the logger used by this pipeline.
    /// </summary>
    private ILogger? Logger { get; }

    /// <summary>
    /// Gets the current execution parameter for the active run.
    /// </summary>
    private TParam Parameter => currentRunContext.Value is { } runContext ? runContext.Parameter : default!;

    /// <summary>
    /// Executes the pipeline synchronously.
    /// </summary>
    /// <param name="parameter">The pipeline input parameter.</param>
    /// <returns>The pipeline result.</returns>
    public TResult? Run(TParam parameter)
    {
        ArgumentNullException.ThrowIfNull(parameter);

        if (operations.Count == 0)
            throw new InvalidOperationException("Must have 1 or more operations configured.");

        var runContext = new RunContext(parameter, configuredResultFactory);
        var startTime = DateTimeOffset.Now;
        long startTimestamp = Stopwatch.GetTimestamp();

        using var activity = PipelineTelemetry.ActivitySource.StartActivity("pipeline.run");
        activity?.SetTag("pipeline.id", Id);
        activity?.SetTag("pipeline.name", Name);

        PipelineTelemetry.PipelineRunCounter.Add(1);

        currentRunContext.Value = runContext;
        LogPipelineStart(startTime);

        try
        {
            var result = RunInternal(runContext, CancellationToken.None);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            PipelineTelemetry.PipelineFailureCounter.Add(1);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("exception.type", ex.GetType().FullName);
            activity?.SetTag("exception.message", ex.Message);
            throw;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            PipelineTelemetry.PipelineDurationMs.Record(elapsed.TotalMilliseconds);
            activity?.SetTag("pipeline.duration.ms", elapsed.TotalMilliseconds);

            LogPipelineFinish(elapsed);
            currentRunContext.Value = null;
            Reset();
        }
    }

    /// <summary>
    /// Executes the pipeline asynchronously.
    /// </summary>
    /// <param name="parameter">The pipeline input parameter.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The pipeline result.</returns>
    /// <exception cref="OperationCanceledException" />
    public async Task<TResult?> RunAsync(TParam parameter, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(parameter);

        if (operations.Count == 0)
            throw new InvalidOperationException("Must have 1 or more operations configured.");

        var runContext = new RunContext(parameter, configuredResultFactory);
        var startTime = DateTimeOffset.Now;
        long startTimestamp = Stopwatch.GetTimestamp();

        using var activity = PipelineTelemetry.ActivitySource.StartActivity("pipeline.run.async");
        activity?.SetTag("pipeline.id", Id);
        activity?.SetTag("pipeline.name", Name);

        PipelineTelemetry.PipelineRunCounter.Add(1);

        currentRunContext.Value = runContext;
        LogPipelineStart(startTime);

        try
        {
            var result = await RunInternalAsync(runContext, cancellationToken).ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return result;
        }
        catch (Exception ex)
        {
            PipelineTelemetry.PipelineFailureCounter.Add(1);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.SetTag("exception.type", ex.GetType().FullName);
            activity?.SetTag("exception.message", ex.Message);
            throw;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
            PipelineTelemetry.PipelineDurationMs.Record(elapsed.TotalMilliseconds);
            activity?.SetTag("pipeline.duration.ms", elapsed.TotalMilliseconds);

            LogPipelineFinish(elapsed);
            currentRunContext.Value = null;
            Reset();
        }
    }

    /// <summary>
    /// Executes the configured pipeline for each input item from an asynchronous source.
    /// </summary>
    /// <param name="parameters">The asynchronous input stream.</param>
    /// <param name="cancellationToken">A cancellation token for batch execution.</param>
    /// <returns>An asynchronous stream of pipeline results in input order.</returns>
    public async IAsyncEnumerable<TResult?> RunBatchAsync(
        IAsyncEnumerable<TParam> parameters,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        await foreach (var parameter in parameters.WithCancellation(cancellationToken).ConfigureAwait(false))
            yield return await RunAsync(parameter, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the configured pipeline for each input item from a memory-backed batch.
    /// </summary>
    /// <param name="parameters">The memory-backed input batch.</param>
    /// <param name="cancellationToken">A cancellation token for batch execution.</param>
    /// <returns>A list of results in input order.</returns>
    public async ValueTask<IReadOnlyList<TResult?>> RunBatchAsync(
        ReadOnlyMemory<TParam> parameters,
        CancellationToken cancellationToken = default)
    {
        if (parameters.IsEmpty)
            return [];

        var results = new TResult?[parameters.Length];

        for (int index = 0; index < parameters.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            results[index] = await RunAsync(parameters.Span[index], cancellationToken).ConfigureAwait(false);
        }

        return results;
    }

    /// <summary>
    /// Adds a typed operation instance.
    /// </summary>
    /// <typeparam name="TInput">The operation input type.</typeparam>
    /// <typeparam name="TOutput">The operation output type.</typeparam>
    /// <param name="operation">The operation to add.</param>
    /// <returns>The current pipeline instance.</returns>
    internal OperationPipeline<TParam, TResult> AddOperation<TInput, TOutput>(Operation<TInput, TOutput> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        operations.Add(operation);
        return this;
    }

    /// <summary>
    /// Adds a synchronous delegate operation.
    /// </summary>
    /// <typeparam name="TInput">The operation input type.</typeparam>
    /// <typeparam name="TOutput">The operation output type.</typeparam>
    /// <param name="execution">The execution delegate.</param>
    /// <param name="operationName">The optional operation name.</param>
    /// <param name="onCompletionHandler">The optional completion callback.</param>
    /// <param name="onExceptionHandler">The optional exception handler callback.</param>
    /// <returns>The current pipeline instance.</returns>
    internal OperationPipeline<TParam, TResult> AddOperation<TInput, TOutput>(Func<TInput, TOutput> execution,
        string? operationName = null,
        Action<TOutput>? onCompletionHandler = null,
        Func<Exception, bool>? onExceptionHandler = null)
    {
        ArgumentNullException.ThrowIfNull(execution);

        AddOperation(
            new DelegateOperation<TInput, TOutput>(
                execution,
                operationName,
                null,
                onCompletionHandler,
                onExceptionHandler));
        return this;
    }

    /// <summary>
    /// Adds an asynchronous delegate operation.
    /// </summary>
    /// <typeparam name="TInput">The operation input type.</typeparam>
    /// <typeparam name="TOutput">The operation output type.</typeparam>
    /// <param name="execution">The asynchronous execution delegate.</param>
    /// <param name="operationName">The optional operation name.</param>
    /// <param name="onCompletionHandler">The optional completion callback.</param>
    /// <param name="onExceptionHandler">The optional exception handler callback.</param>
    /// <returns>The current pipeline instance.</returns>
    internal OperationPipeline<TParam, TResult> AddOperation<TInput, TOutput>(
        Func<TInput, CancellationToken, ValueTask<TOutput?>> execution,
        string? operationName = null,
        Action<TOutput>? onCompletionHandler = null,
        Func<Exception, bool>? onExceptionHandler = null)
    {
        ArgumentNullException.ThrowIfNull(execution);

        AddOperation(
            new DelegateOperation<TInput, TOutput>(
                execution,
                operationName,
                null,
                onCompletionHandler,
                onExceptionHandler));
        return this;
    }

    /// <summary>
    /// Sets the final result factory for the pipeline.
    /// </summary>
    /// <param name="pipelineResultFactory">The result factory delegate.</param>
    /// <returns>The current pipeline instance.</returns>
    internal OperationPipeline<TParam, TResult> SetResult(Func<TResult?> pipelineResultFactory)
    {
        ArgumentNullException.ThrowIfNull(pipelineResultFactory);

        var runContext = currentRunContext.Value;

        if (runContext != null)
        {
            if (runContext.ResultFactory != null)
                throw new InvalidOperationException("You can only set the result once.");

            runContext.ResultFactory = pipelineResultFactory;
            return this;
        }

        if (configuredResultFactory != null)
            throw new InvalidOperationException("You can only set the result once.");

        configuredResultFactory = pipelineResultFactory;
        return this;
    }

    /// <summary>
    /// Logs operation completion information.
    /// </summary>
    /// <param name="operationName">The operation name.</param>
    /// <param name="elapsed">The elapsed execution time.</param>
    private void LogOperationFinish(string operationName, TimeSpan elapsed) => Logger?.LogInformation(
        "Pipeline [{PipelineId}] operation `{OperationName}` ended in {ElapsedMs}ms",
        Id,
        operationName,
        elapsed.TotalMilliseconds);

    /// <summary>
    /// Logs operation start information.
    /// </summary>
    /// <param name="operationName">The operation name.</param>
    /// <param name="now">The start timestamp.</param>
    private void LogOperationStart(string operationName, DateTimeOffset now) => Logger?.LogInformation(
        "Pipeline [{PipelineId}] operation `{OperationName}` starting at {Now}...",
        Id,
        operationName,
        now);

    /// <summary>
    /// Logs pipeline completion information.
    /// </summary>
    /// <param name="elapsed">The elapsed execution time.</param>
    private void LogPipelineFinish(TimeSpan elapsed) => Logger?.LogInformation(
        "Pipeline [{PipelineId}](`{PipelineName}`) ended in {ElapsedMs}ms",
        Id,
        Name,
        elapsed.TotalMilliseconds);

    /// <summary>
    /// Logs pipeline start information.
    /// </summary>
    /// <param name="now">The start timestamp.</param>
    private void LogPipelineStart(DateTimeOffset now) =>
        Logger?.LogInformation("Pipeline [{PipelineId}](`{PipelineName}`) starting at {Now}...", Id, Name, now);

    internal void SetDefinition(PipelineDefinition pipelineDefinition)
    {
        ArgumentNullException.ThrowIfNull(pipelineDefinition);
        definition = pipelineDefinition;
    }

    private void Reset() => configuredResultFactory = null;

    private TResult? RunInternal(RunContext runContext, CancellationToken cancellationToken = default)
    {
        var firstOperation = operations.First();
        var lastOperation = operations.Last();

        object? operationResult = default(TResult?);

        foreach (var operation in operations.Where(operation => operation.CanExecute()))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var startTime = DateTimeOffset.Now;
            long startTimestamp = Stopwatch.GetTimestamp();

            using var operationActivity = PipelineTelemetry.ActivitySource.StartActivity("pipeline.operation.run");
            operationActivity?.SetTag("pipeline.id", Id);
            operationActivity?.SetTag("pipeline.name", Name);
            operationActivity?.SetTag("pipeline.operation.name", operation.Name);

            LogOperationStart(operation.Name, startTime);

            try
            {
                object? operationParameter = operation == firstOperation
                    ? runContext.Parameter
                    : operationResult;

                operationResult = operation.UntypedExecution(operationParameter);

                operation.UntypedOnCompletionHandler?.Invoke(operationResult);
            }
            catch (Exception ex)
            {
                operationActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                operationActivity?.SetTag("exception.type", ex.GetType().FullName);
                operationActivity?.SetTag("exception.message", ex.Message);

                if (!operation.OnExceptionHandler?.Invoke(ex) ?? true)
                    throw;
            }
            finally
            {
                if (runContext.ResultFactory == null && operation == lastOperation)
                {
                    object? finalOperationResult = operationResult;
                    SetResult(() => (TResult?)finalOperationResult);
                }

                var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
                PipelineTelemetry.PipelineOperationDurationMs.Record(elapsed.TotalMilliseconds);
                operationActivity?.SetTag("pipeline.operation.duration.ms", elapsed.TotalMilliseconds);

                LogOperationFinish(operation.Name, elapsed);
            }
        }

        return runContext.ResultFactory!();
    }

    private async Task<TResult?> RunInternalAsync(RunContext runContext, CancellationToken cancellationToken = default)
    {
        var firstOperation = operations.First();
        var lastOperation = operations.Last();

        object? operationResult = default(TResult?);

        foreach (var operation in operations.Where(operation => operation.CanExecute()))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var startTime = DateTimeOffset.Now;
            long startTimestamp = Stopwatch.GetTimestamp();

            using var operationActivity =
                PipelineTelemetry.ActivitySource.StartActivity("pipeline.operation.run.async");
            operationActivity?.SetTag("pipeline.id", Id);
            operationActivity?.SetTag("pipeline.name", Name);
            operationActivity?.SetTag("pipeline.operation.name", operation.Name);

            LogOperationStart(operation.Name, startTime);

            try
            {
                object? operationParameter = operation == firstOperation
                    ? runContext.Parameter
                    : operationResult;

                operationResult = await operation.UntypedExecutionAsync(operationParameter, cancellationToken)
                    .ConfigureAwait(false);

                operation.UntypedOnCompletionHandler?.Invoke(operationResult);
            }
            catch (Exception ex)
            {
                operationActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                operationActivity?.SetTag("exception.type", ex.GetType().FullName);
                operationActivity?.SetTag("exception.message", ex.Message);

                if (!operation.OnExceptionHandler?.Invoke(ex) ?? true)
                    throw;
            }
            finally
            {
                if (runContext.ResultFactory == null && operation == lastOperation)
                {
                    object? finalOperationResult = operationResult;
                    SetResult(() => (TResult?)finalOperationResult);
                }

                var elapsed = Stopwatch.GetElapsedTime(startTimestamp);
                PipelineTelemetry.PipelineOperationDurationMs.Record(elapsed.TotalMilliseconds);
                operationActivity?.SetTag("pipeline.operation.duration.ms", elapsed.TotalMilliseconds);

                LogOperationFinish(operation.Name, elapsed);
            }
        }

        return runContext.ResultFactory!();
    }

    private sealed class RunContext(TParam parameter, Func<TResult?>? resultFactory)
    {
        public TParam Parameter { get; } = parameter;

        public Func<TResult?>? ResultFactory { get; set; } = resultFactory;
    }
}