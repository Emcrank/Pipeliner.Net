using System;
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
    private readonly PipelineGraph graph;
    private readonly ILogger? logger;
    private readonly Func<TState> stateFactory;

    internal StatefulPipelineBuilder(
        Func<TInput, TState, CancellationToken, ValueTask<TCurrent>> chain,
        ILogger? logger,
        PipelineGraph graph,
        Func<TState> stateFactory)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(stateFactory);

        this.chain = chain;
        this.logger = logger;
        this.graph = graph;
        this.stateFactory = stateFactory;
    }

    /// <summary>
    /// Builds an <see cref="OperationPipeline{TParam,TResult}" /> from the current stateful chain.
    /// </summary>
    /// <param name="pipelineName">Optional pipeline name.</param>
    /// <returns>The built operation pipeline.</returns>
    public OperationPipeline<TInput, TCurrent> Build(string? pipelineName = null)
    {
        var pipeline = new OperationPipeline<TInput, TCurrent>(logger)
            .AddOperation<TInput, TCurrent>(async (input, cancellationToken) =>
            {
                var state = stateFactory();
                ArgumentNullException.ThrowIfNull(state);

                return await PipelineSagaContext.RunAsync(
                    token => chain(input, state, token),
                    cancellationToken).ConfigureAwait(false);
            });

        if (!string.IsNullOrWhiteSpace(pipelineName))
            pipeline.Name = pipelineName;

        pipeline.SetDefinition(graph.ToDefinition(pipeline.Id, pipeline.Name));
        return pipeline;
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
        var concurrencyGate = effectiveOptions.MaxConcurrency is { } maxConcurrency
            ? new SemaphoreSlim(maxConcurrency, maxConcurrency)
            : null;

        return new StatefulPipelineBuilder<TInput, TNext, TState>(
            async (input, state, cancellationToken) =>
            {
                var currentValue = await chain(input, state, cancellationToken).ConfigureAwait(false);
                var traceContext = PipelineTraceContext.Current;
                if (traceContext is null)
                    return await ExecuteWithConcurrencyAsync(currentValue, state, cancellationToken).ConfigureAwait(false);

                return await traceContext.TraceStepAsync(
                    effectiveStepName ?? PipelineNodeKind.Step.ToString(),
                    PipelineNodeKind.Step,
                    typeof(TCurrent),
                    typeof(TNext),
                    () => ExecuteWithConcurrencyAsync(currentValue, state, cancellationToken)).ConfigureAwait(false);
            },
            logger,
            graph.AddStep(effectiveStepName, typeof(TCurrent), typeof(TNext), PipelineNodeKind.Step),
            stateFactory);

        async ValueTask<TNext> ExecuteUserStepAsync(TCurrent currentValue, TState state, CancellationToken cancellationToken) =>
            await step(currentValue, state, cancellationToken).ConfigureAwait(false);

        async ValueTask<TNext> ExecuteWithPolicyAsync(TCurrent currentValue, TState state, CancellationToken cancellationToken)
        {
            if (effectiveOptions.Policy is { } policy)
                return await policy.ExecuteAsync(token => ExecuteUserStepAsync(currentValue, state, token), cancellationToken)
                    .ConfigureAwait(false);

            return await ExecuteUserStepAsync(currentValue, state, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithRateLimitAsync(TCurrent currentValue, TState state, CancellationToken cancellationToken)
        {
            if (effectiveOptions.RateLimiter is not { } rateLimiter)
                return await ExecuteWithPolicyAsync(currentValue, state, cancellationToken).ConfigureAwait(false);

            using var lease = await rateLimiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
            if (!lease.IsAcquired)
                throw new PipelineRateLimitRejectedException(effectiveStepName ?? "Unnamed step");

            return await ExecuteWithPolicyAsync(currentValue, state, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithConcurrencyAsync(TCurrent currentValue, TState state, CancellationToken cancellationToken)
        {
            if (concurrencyGate is null)
                return await ExecuteWithRateLimitAsync(currentValue, state, cancellationToken).ConfigureAwait(false);

            await concurrencyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await ExecuteWithRateLimitAsync(currentValue, state, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                concurrencyGate.Release();
            }
        }
    }
}