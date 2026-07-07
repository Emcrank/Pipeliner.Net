using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Pipeliner.Net;

/// <summary>
/// Represents a type-threaded pipeline builder with per-run state.
/// </summary>
/// <typeparam name="TInput">The original pipeline input type.</typeparam>
/// <typeparam name="TCurrent">The current output type after applied steps.</typeparam>
/// <typeparam name="TState">The per-run state type.</typeparam>
public sealed class StatefulPipelineBuilder<TInput, TCurrent, TState>
{
    private readonly Func<TInput, TState, CancellationToken, ValueTask<TCurrent>> chain;
    private readonly PipelineCheckpointOptions? checkpointOptions;
    private readonly PipelineGraph graph;
    private readonly ILogger? logger;
    private readonly IReadOnlyList<IPipelineObserver> observers;
    private readonly Func<TState> stateFactory;

    internal StatefulPipelineBuilder(
        Func<TInput, TState, CancellationToken, ValueTask<TCurrent>> chain,
        ILogger? logger,
        PipelineGraph graph,
        Func<TState> stateFactory,
        PipelineCheckpointOptions? checkpointOptions = null,
        IReadOnlyList<IPipelineObserver>? observers = null)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(stateFactory);

        this.chain = chain;
        this.logger = logger;
        this.graph = graph;
        this.stateFactory = stateFactory;
        this.checkpointOptions = checkpointOptions;
        this.observers = observers ?? [];
    }

    /// <summary>
    /// Builds an <see cref="OperationPipeline{TParam,TResult}" /> from the current stateful chain.
    /// </summary>
    /// <param name="pipelineName">Optional pipeline name.</param>
    /// <param name="pipelineVersion">Optional pipeline definition version.</param>
    /// <returns>The built operation pipeline.</returns>
    public OperationPipeline<TInput, TCurrent> Build(string? pipelineName = null, string? pipelineVersion = null)
    {
        var pipeline = new OperationPipeline<TInput, TCurrent>(logger);

        if (!string.IsNullOrWhiteSpace(pipelineName))
            pipeline.Name = pipelineName;

        var effectivePipelineVersion = string.IsNullOrWhiteSpace(pipelineVersion) ? "1.0.0" : pipelineVersion;
        pipeline.Version = effectivePipelineVersion;

        pipeline.AddOperation<TInput, TCurrent>(async (input, cancellationToken) =>
        {
            var state = stateFactory();
            ArgumentNullException.ThrowIfNull(state);

            var executionContext = new PipelineExecutionContext(
                Guid.NewGuid().ToString("D"),
                pipeline.Id,
                pipeline.Name,
                effectivePipelineVersion,
                checkpointOptions,
                observers);

            var startedAt = DateTimeOffset.UtcNow;
            long started = System.Diagnostics.Stopwatch.GetTimestamp();
            await executionContext.NotifyRunStartedAsync(startedAt, cancellationToken).ConfigureAwait(false);

            try
            {
                var result = await PipelineExecutionContext.RunAsync(
                    executionContext,
                    token => PipelineSagaContext.RunAsync(
                        sagaToken => chain(input, state, sagaToken),
                        token),
                    cancellationToken).ConfigureAwait(false);

                await executionContext.NotifyRunCompletedAsync(
                    startedAt,
                    DateTimeOffset.UtcNow,
                    System.Diagnostics.Stopwatch.GetElapsedTime(started),
                    cancellationToken).ConfigureAwait(false);

                return result;
            }
            catch (PipelineHaltedException exception)
            {
                await executionContext.NotifyRunHaltedAsync(exception, cancellationToken).ConfigureAwait(false);
                throw;
            }
            catch (Exception exception)
            {
                await executionContext.NotifyRunFailedAsync(
                    startedAt,
                    DateTimeOffset.UtcNow,
                    System.Diagnostics.Stopwatch.GetElapsedTime(started),
                    exception,
                    cancellationToken).ConfigureAwait(false);
                throw;
            }
        });

        pipeline.SetDefinition(graph.ToDefinition(pipeline.Id, pipeline.Name, effectivePipelineVersion));
        return pipeline;
    }

    /// <summary>
    /// Adds an observer for run, step, halt, and checkpoint events.
    /// </summary>
    /// <param name="observer">The observer to add.</param>
    /// <returns>A new builder with the observer configured.</returns>
    public StatefulPipelineBuilder<TInput, TCurrent, TState> WithObserver(IPipelineObserver observer)
    {
        ArgumentNullException.ThrowIfNull(observer);

        return new StatefulPipelineBuilder<TInput, TCurrent, TState>(
            chain,
            logger,
            graph,
            stateFactory,
            checkpointOptions,
            observers.Concat([observer]).ToArray());
    }

    /// <summary>
    /// Appends a synchronous state-aware step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The synchronous state-aware step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public StatefulPipelineBuilder<TInput, TNext, TState> Then<TNext>(Func<TCurrent, TState, TNext> step) =>
        Then(null, step);

    /// <summary>
    /// Appends a synchronous state-aware step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The synchronous state-aware step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public StatefulPipelineBuilder<TInput, TNext, TState> Then<TNext>(
        string? stepName,
        Func<TCurrent, TState, TNext> step)
    {
        ArgumentNullException.ThrowIfNull(step);

        return ThenAsync(stepName, (current, state, _) => ValueTask.FromResult(step(current, state)));
    }

    /// <summary>
    /// Appends a synchronous stateless step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The synchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public StatefulPipelineBuilder<TInput, TNext, TState> Then<TNext>(Func<TCurrent, TNext> step) =>
        Then(null, step);

    /// <summary>
    /// Appends a synchronous stateless step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The synchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public StatefulPipelineBuilder<TInput, TNext, TState> Then<TNext>(
        string? stepName,
        Func<TCurrent, TNext> step)
    {
        ArgumentNullException.ThrowIfNull(step);

        return ThenAsync(stepName, (current, _, _) => ValueTask.FromResult(step(current)));
    }

    /// <summary>
    /// Configures checkpoint persistence for this request-response pipeline.
    /// </summary>
    /// <param name="options">The checkpoint options.</param>
    /// <returns>A new builder with checkpoint persistence configured.</returns>
    public StatefulPipelineBuilder<TInput, TCurrent, TState> WithCheckpointing(PipelineCheckpointOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new StatefulPipelineBuilder<TInput, TCurrent, TState>(
            chain,
            logger,
            graph,
            stateFactory,
            options,
            observers);
    }

    /// <summary>
    /// Configures checkpoint persistence for this request-response pipeline.
    /// </summary>
    /// <param name="store">The checkpoint store.</param>
    /// <param name="failureBehavior">The checkpoint persistence failure behavior.</param>
    /// <returns>A new builder with checkpoint persistence configured.</returns>
    public StatefulPipelineBuilder<TInput, TCurrent, TState> WithCheckpointing(
        IPipelineCheckpointStore store,
        PipelineCheckpointFailureBehavior failureBehavior = PipelineCheckpointFailureBehavior.FailRun)
    {
        ArgumentNullException.ThrowIfNull(store);

        return WithCheckpointing(new PipelineCheckpointOptions(store, failureBehavior: failureBehavior));
    }

    /// <summary>
    /// Adds a durable checkpoint for the current value.
    /// </summary>
    /// <param name="checkpointName">The checkpoint display name.</param>
    /// <returns>A new builder with a checkpoint identity step.</returns>
    public StatefulPipelineBuilder<TInput, TCurrent, TState> Checkpoint(string checkpointName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointName);

        var nextGraph = graph.AddStep(checkpointName, typeof(TCurrent), typeof(TCurrent), PipelineNodeKind.Checkpoint);
        var checkpointNode = nextGraph.TerminalNode;

        return new StatefulPipelineBuilder<TInput, TCurrent, TState>(
            async (input, state, cancellationToken) =>
            {
                var current = await chain(input, state, cancellationToken).ConfigureAwait(false);
                if (PipelineExecutionContext.Current is { } context)
                {
                    await context.ExecuteStepAsync(
                            checkpointNode.Id,
                            checkpointNode.Name,
                            checkpointNode.Kind,
                            async (stepContext, token) =>
                            {
                                await stepContext.SaveCheckpointAsync(checkpointName, checkpointNode.Id, current, token)
                                    .ConfigureAwait(false);
                                return current;
                            },
                            cancellationToken)
                        .ConfigureAwait(false);
                }

                return current;
            },
            logger,
            nextGraph,
            stateFactory,
            checkpointOptions,
            observers);
    }

    /// <summary>
    /// Appends an asynchronous state-aware step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The asynchronous state-aware step delegate.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public StatefulPipelineBuilder<TInput, TNext, TState> ThenAsync<TNext>(
        Func<TCurrent, TState, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options = null) =>
        ThenAsync(null, step, options);

    /// <summary>
    /// Appends an asynchronous state-aware step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The asynchronous state-aware step delegate.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public StatefulPipelineBuilder<TInput, TNext, TState> ThenAsync<TNext>(
        string? stepName,
        Func<TCurrent, TState, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(step);

        var effectiveOptions = options ?? StepExecutionOptions.None();
        var effectiveStepName = stepName ?? effectiveOptions.Name;
        var nextGraph = graph.AddStep(effectiveStepName, typeof(TCurrent), typeof(TNext), PipelineNodeKind.Step);
        var stepNode = nextGraph.TerminalNode;
        var concurrencyGate = effectiveOptions.MaxConcurrency is { } maxConcurrency
            ? new SemaphoreSlim(maxConcurrency, maxConcurrency)
            : null;

        return new StatefulPipelineBuilder<TInput, TNext, TState>(
            async (input, state, cancellationToken) =>
            {
                var currentValue = await chain(input, state, cancellationToken).ConfigureAwait(false);

                async ValueTask<TNext> ExecuteStepBodyAsync(PipelineExecutionContext stepContext, CancellationToken token)
                {
                    var traceContext = PipelineTraceContext.Current;
                    if (traceContext is null)
                        return await ExecuteWithConcurrencyAsync(currentValue, state, stepContext, token).ConfigureAwait(false);

                    return await traceContext.TraceStepAsync(
                        stepNode.Name,
                        stepNode.Kind,
                        typeof(TCurrent),
                        typeof(TNext),
                        () => ExecuteWithConcurrencyAsync(currentValue, state, stepContext, token)).ConfigureAwait(false);
                }

                if (PipelineExecutionContext.Current is { } context)
                {
                    return await context.ExecuteStepAsync(
                            stepNode.Id,
                            stepNode.Name,
                            stepNode.Kind,
                            ExecuteStepBodyAsync,
                            cancellationToken)
                        .ConfigureAwait(false);
                }

                var fallbackContext = new PipelineExecutionContext(
                    Guid.NewGuid().ToString("D"),
                    "fallback-pipeline",
                    "Fallback Pipeline",
                    "1.0.0",
                    null);

                return await ExecuteStepBodyAsync(fallbackContext, cancellationToken).ConfigureAwait(false);
            },
            logger,
            nextGraph,
            stateFactory,
            checkpointOptions,
            observers);

        async ValueTask<TNext> ExecuteUserStepAsync(
            TCurrent currentValue,
            TState state,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken) =>
            await step(currentValue, state, cancellationToken).ConfigureAwait(false);

        async ValueTask<TNext> ExecuteWithPolicyAsync(
            TCurrent currentValue,
            TState state,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken)
        {
            if (effectiveOptions.Policy is { } policy)
                return await policy.ExecuteAsync(
                        token => ExecuteUserStepAsync(currentValue, state, stepContext, token),
                        cancellationToken)
                    .ConfigureAwait(false);

            return await ExecuteUserStepAsync(currentValue, state, stepContext, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithRateLimitAsync(
            TCurrent currentValue,
            TState state,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken)
        {
            if (effectiveOptions.RateLimiter is not { } rateLimiter)
                return await ExecuteWithPolicyAsync(currentValue, state, stepContext, cancellationToken).ConfigureAwait(false);

            using var lease = await rateLimiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
            if (!lease.IsAcquired)
                throw new PipelineRateLimitRejectedException(stepNode.Name);

            return await ExecuteWithPolicyAsync(currentValue, state, stepContext, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithConcurrencyAsync(
            TCurrent currentValue,
            TState state,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken)
        {
            if (concurrencyGate is null)
                return await ExecuteWithRateLimitAsync(currentValue, state, stepContext, cancellationToken).ConfigureAwait(false);

            await concurrencyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await ExecuteWithRateLimitAsync(currentValue, state, stepContext, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                concurrencyGate.Release();
            }
        }
    }
}