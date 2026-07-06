using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Pipeliner.Net;

/// <summary>
/// Represents a type-threaded pipeline builder.
/// </summary>
/// <typeparam name="TInput">The original pipeline input type.</typeparam>
/// <typeparam name="TCurrent">The current output type after applied steps.</typeparam>
public sealed class PipelineBuilder<TInput, TCurrent>
{
    private readonly Func<TInput, CancellationToken, ValueTask<TCurrent>> chain;
    private readonly PipelineGraph graph;
    private readonly ILogger? logger;

    /// <summary>
    /// Initializes a new instance of <see cref="PipelineBuilder{TInput,TCurrent}" />.
    /// </summary>
    /// <param name="chain">The pipeline execution chain.</param>
    /// <param name="logger">The optional logger used by built pipelines.</param>
    /// <param name="graph">The pipeline definition graph.</param>
    internal PipelineBuilder(
        Func<TInput, CancellationToken, ValueTask<TCurrent>> chain,
        ILogger? logger,
        PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(graph);

        this.chain = chain;
        this.logger = logger;
        this.graph = graph;
    }

    /// <summary>
    /// Branches execution based on a predicate.
    /// </summary>
    /// <typeparam name="TNext">The output type of both branches.</typeparam>
    /// <param name="predicate">The branch condition.</param>
    /// <param name="whenTrue">The branch executed when predicate is true.</param>
    /// <param name="whenFalse">The branch executed when predicate is false.</param>
    /// <returns>A new builder with the branch output type.</returns>
    public PipelineBuilder<TInput, TNext> Branch<TNext>(
        Func<TCurrent, bool> predicate,
        Func<TCurrent, TNext> whenTrue,
        Func<TCurrent, TNext> whenFalse) =>
        Branch(null, predicate, whenTrue, whenFalse);

    /// <summary>
    /// Branches execution based on a predicate.
    /// </summary>
    /// <typeparam name="TNext">The output type of both branches.</typeparam>
    /// <param name="stepName">The branch display name used in pipeline descriptions.</param>
    /// <param name="predicate">The branch condition.</param>
    /// <param name="whenTrue">The branch executed when predicate is true.</param>
    /// <param name="whenFalse">The branch executed when predicate is false.</param>
    /// <returns>A new builder with the branch output type.</returns>
    public PipelineBuilder<TInput, TNext> Branch<TNext>(
        string? stepName,
        Func<TCurrent, bool> predicate,
        Func<TCurrent, TNext> whenTrue,
        Func<TCurrent, TNext> whenFalse)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(whenTrue);
        ArgumentNullException.ThrowIfNull(whenFalse);

        return BranchAsync(
            stepName,
            predicate,
            (current, _) => ValueTask.FromResult(whenTrue(current)),
            (current, _) => ValueTask.FromResult(whenFalse(current)));
    }

    /// <summary>
    /// Branches execution based on a predicate.
    /// </summary>
    /// <typeparam name="TNext">The output type of both branches.</typeparam>
    /// <param name="predicate">The branch condition.</param>
    /// <param name="whenTrue">The branch executed when predicate is true.</param>
    /// <param name="whenFalse">The branch executed when predicate is false.</param>
    /// <returns>A new builder with the branch output type.</returns>
    public PipelineBuilder<TInput, TNext> BranchAsync<TNext>(
        Func<TCurrent, bool> predicate,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> whenTrue,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> whenFalse) =>
        BranchAsync(null, predicate, whenTrue, whenFalse);

    /// <summary>
    /// Branches execution based on a predicate.
    /// </summary>
    /// <typeparam name="TNext">The output type of both branches.</typeparam>
    /// <param name="stepName">The branch display name used in pipeline descriptions.</param>
    /// <param name="predicate">The branch condition.</param>
    /// <param name="whenTrue">The branch executed when predicate is true.</param>
    /// <param name="whenFalse">The branch executed when predicate is false.</param>
    /// <returns>A new builder with the branch output type.</returns>
    public PipelineBuilder<TInput, TNext> BranchAsync<TNext>(
        string? stepName,
        Func<TCurrent, bool> predicate,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> whenTrue,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> whenFalse)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(whenTrue);
        ArgumentNullException.ThrowIfNull(whenFalse);

        return new PipelineBuilder<TInput, TNext>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                return predicate(currentValue)
                    ? await whenTrue(currentValue, cancellationToken).ConfigureAwait(false)
                    : await whenFalse(currentValue, cancellationToken).ConfigureAwait(false);
            },
            logger,
            graph.AddBranch(stepName, typeof(TCurrent), typeof(TNext)));
    }

    /// <summary>
    /// Builds an <see cref="OperationPipeline{TParam, TResult}" /> from the current chain.
    /// </summary>
    /// <param name="pipelineName">Optional pipeline name.</param>
    /// <returns>The built operation pipeline.</returns>
    public OperationPipeline<TInput, TCurrent> Build(string? pipelineName = null)
    {
        var pipeline = new OperationPipeline<TInput, TCurrent>(logger)
            .AddOperation<TInput, TCurrent>(async (input, cancellationToken) =>
                await chain(input, cancellationToken).ConfigureAwait(false));

        if (!string.IsNullOrWhiteSpace(pipelineName))
            pipeline.Name = pipelineName;

        pipeline.SetDefinition(graph.ToDefinition(pipeline.Id, pipeline.Name));
        return pipeline;
    }

    /// <summary>
    /// Executes multiple branches in parallel for the current value.
    /// </summary>
    /// <typeparam name="TBranch">The branch output type.</typeparam>
    /// <param name="branches">The branch delegates to execute in parallel.</param>
    /// <returns>A new builder whose output is a fork execution result.</returns>
    public PipelineBuilder<TInput, ForkExecutionResult<TBranch>> Fork<TBranch>(
        params Func<TCurrent, CancellationToken, ValueTask<TBranch>>?[] branches) =>
        Fork(null, branches);

    /// <summary>
    /// Executes multiple branches in parallel for the current value.
    /// </summary>
    /// <typeparam name="TBranch">The branch output type.</typeparam>
    /// <param name="stepName">The fork display name used in pipeline descriptions.</param>
    /// <param name="branches">The branch delegates to execute in parallel.</param>
    /// <returns>A new builder whose output is a fork execution result.</returns>
    public PipelineBuilder<TInput, ForkExecutionResult<TBranch>> Fork<TBranch>(
        string? stepName,
        params Func<TCurrent, CancellationToken, ValueTask<TBranch>>?[] branches)
    {
        ArgumentNullException.ThrowIfNull(branches);

        if (branches.Length == 0)
            throw new ArgumentException("At least one branch must be provided.", nameof(branches));

        if (branches.Any(branch => branch is null))
            throw new ArgumentException("All branches must be non-null.", nameof(branches));

        return new PipelineBuilder<TInput, ForkExecutionResult<TBranch>>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                var branchTasks = new Task<ForkResult<TBranch>>[branches.Length];

                for (int index = 0; index < branches.Length; index++)
                {
                    var currentBranch = branches[index]!;
                    branchTasks[index] = ExecuteBranchAsync(currentBranch, currentValue, index, cancellationToken);
                }

                var branchResults = await Task.WhenAll(branchTasks).ConfigureAwait(false);
                return new ForkExecutionResult<TBranch>(branchResults);
            },
            logger,
            graph.AddFork(
                stepName,
                typeof(TCurrent),
                typeof(TBranch),
                typeof(ForkExecutionResult<TBranch>),
                branches.Length));

        static async Task<ForkResult<TBranch>> ExecuteBranchAsync(
            Func<TCurrent, CancellationToken, ValueTask<TBranch>> branch,
            TCurrent currentValue,
            int index,
            CancellationToken cancellationToken)
        {
            try
            {
                var value = await branch(currentValue, cancellationToken).ConfigureAwait(false);
                return new ForkResult<TBranch>(index, true, value, null);
            }
            catch (Exception exception)
            {
                return new ForkResult<TBranch>(index, false, default, exception);
            }
        }
    }

    /// <summary>
    /// Merges forked branch outputs into a single value.
    /// </summary>
    /// <typeparam name="TBranch">The branch output type.</typeparam>
    /// <typeparam name="TNext">The merge output type.</typeparam>
    /// <param name="mergeStep">The merge delegate.</param>
    /// <param name="options">The merge options.</param>
    /// <returns>A new builder with the merge output type.</returns>
    public PipelineBuilder<TInput, TNext> Merge<TBranch, TNext>(
        Func<IReadOnlyList<TBranch>, CancellationToken, ValueTask<TNext>> mergeStep,
        MergeStepOptions? options = null) =>
        Merge(null, mergeStep, options);

    /// <summary>
    /// Merges forked branch outputs into a single value.
    /// </summary>
    /// <typeparam name="TBranch">The branch output type.</typeparam>
    /// <typeparam name="TNext">The merge output type.</typeparam>
    /// <param name="stepName">The merge display name used in pipeline descriptions.</param>
    /// <param name="mergeStep">The merge delegate.</param>
    /// <param name="options">The merge options.</param>
    /// <returns>A new builder with the merge output type.</returns>
    public PipelineBuilder<TInput, TNext> Merge<TBranch, TNext>(
        string? stepName,
        Func<IReadOnlyList<TBranch>, CancellationToken, ValueTask<TNext>> mergeStep,
        MergeStepOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(mergeStep);

        var effectiveOptions = options ?? MergeStepOptions.CustomReducer();

        return new PipelineBuilder<TInput, TNext>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                if (currentValue is not ForkExecutionResult<TBranch> forkExecutionResult)
                    throw new InvalidOperationException($"{nameof(Merge)} must be called after {nameof(Fork)}.");

                if (effectiveOptions.ConflictStrategy == MergeConflictStrategy.ThrowOnAnyFailure)
                {
                    var successfulResults = await MergeReducers
                        .ThrowOnAnyFailureAsync(forkExecutionResult.BranchResults, cancellationToken)
                        .ConfigureAwait(false);
                    if (successfulResults is TNext typedResults)
                        return typedResults;

                    throw new InvalidOperationException(
                        $"{nameof(MergeConflictStrategy.ThrowOnAnyFailure)} requires {typeof(TNext).Name} to be assignable from {typeof(IReadOnlyList<TBranch>).Name}.");
                }

                if (effectiveOptions.ConflictStrategy == MergeConflictStrategy.IgnoreFailures)
                {
                    var successfulResults = await MergeReducers
                        .IgnoreFailuresAsync(forkExecutionResult.BranchResults, cancellationToken)
                        .ConfigureAwait(false);
                    if (successfulResults is TNext typedResults)
                        return typedResults;

                    throw new InvalidOperationException(
                        $"{nameof(MergeConflictStrategy.IgnoreFailures)} requires {typeof(TNext).Name} to be assignable from {typeof(IReadOnlyList<TBranch>).Name}.");
                }

                if (effectiveOptions.ConflictStrategy == MergeConflictStrategy.TakeFirst)
                {
                    var firstResult = await MergeReducers
                        .TakeFirstAsync(forkExecutionResult.BranchResults, cancellationToken).ConfigureAwait(false);
                    if (firstResult is TNext typedFirstResult)
                        return typedFirstResult;

                    throw new InvalidOperationException(
                        $"{nameof(MergeConflictStrategy.TakeFirst)} requires {typeof(TNext).Name} to match {typeof(TBranch).Name}.");
                }

                var successfulResultsForMerge = await MergeReducers
                    .IgnoreFailuresAsync(forkExecutionResult.BranchResults, cancellationToken).ConfigureAwait(false);
                if (successfulResultsForMerge.Count == 0)
                    throw new AggregateException(forkExecutionResult.Failures);

                return await mergeStep(successfulResultsForMerge, cancellationToken).ConfigureAwait(false);
            },
            logger,
            graph.AddStep(stepName, typeof(ForkExecutionResult<TBranch>), typeof(TNext), PipelineNodeKind.Merge));
    }

    /// <summary>
    /// Appends a synchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The synchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> Then<TNext>(Func<TCurrent, TNext> step) =>
        Then(null, step);

    /// <summary>
    /// Appends a synchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The synchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> Then<TNext>(string? stepName, Func<TCurrent, TNext> step)
    {
        ArgumentNullException.ThrowIfNull(step);

        return ThenAsync(stepName, (current, _) => ValueTask.FromResult(step(current)));
    }

    /// <summary>
    /// Appends an asynchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The asynchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> Then<TNext>(Func<TCurrent, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsync(step);

    /// <summary>
    /// Appends an asynchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The asynchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> Then<TNext>(
        string? stepName,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsync(stepName, step);

    /// <summary>
    /// Appends a step produced by a factory.
    /// </summary>
    /// <typeparam name="TStep">The step type.</typeparam>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepFactory">Factory used to create the step instance.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> Then<TStep, TNext>(Func<TStep> stepFactory)
        where TStep : IPipelineStep<TCurrent, TNext> =>
        Then<TStep, TNext>(typeof(TStep).Name, stepFactory);

    /// <summary>
    /// Appends a step produced by a factory.
    /// </summary>
    /// <typeparam name="TStep">The step type.</typeparam>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="stepFactory">Factory used to create the step instance.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> Then<TStep, TNext>(string stepName, Func<TStep> stepFactory)
        where TStep : IPipelineStep<TCurrent, TNext>
    {
        ArgumentNullException.ThrowIfNull(stepFactory);

        return ThenAsync(stepName, async (value, cancellationToken) =>
        {
            var step = stepFactory();
            ArgumentNullException.ThrowIfNull(step);
            return await step.ExecuteAsync(value, cancellationToken).ConfigureAwait(false);
        });
    }

    /// <summary>
    /// Appends an asynchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The asynchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(Func<TCurrent, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsyncCore(null, step, StepExecutionOptions.None());

    /// <summary>
    /// Appends an asynchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The asynchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        string? stepName,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsyncCore(stepName, step, StepExecutionOptions.None());

    /// <summary>
    /// Appends an asynchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The asynchronous step delegate.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        Func<TCurrent, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options) =>
        ThenAsyncCore(null, step, options);

    /// <summary>
    /// Appends an asynchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The asynchronous step delegate.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        string? stepName,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options) =>
        ThenAsyncCore(stepName, step, options);

    /// <summary>
    /// Appends a parallel projection step for sequence outputs.
    /// </summary>
    /// <typeparam name="TItem">The source sequence item type.</typeparam>
    /// <typeparam name="TNext">The destination item type.</typeparam>
    /// <param name="step">The per-item asynchronous step.</param>
    /// <param name="options">Parallel execution options.</param>
    /// <returns>A new builder whose output is the projected ordered results.</returns>
    public PipelineBuilder<TInput, IReadOnlyList<TNext>> ThenParallel<TItem, TNext>(
        Func<TItem, CancellationToken, ValueTask<TNext>> step,
        ParallelStepOptions? options = null) =>
        ThenParallel(null, step, options);

    /// <summary>
    /// Appends a parallel projection step for sequence outputs.
    /// </summary>
    /// <typeparam name="TItem">The source sequence item type.</typeparam>
    /// <typeparam name="TNext">The destination item type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The per-item asynchronous step.</param>
    /// <param name="options">Parallel execution options.</param>
    /// <returns>A new builder whose output is the projected ordered results.</returns>
    public PipelineBuilder<TInput, IReadOnlyList<TNext>> ThenParallel<TItem, TNext>(
        string? stepName,
        Func<TItem, CancellationToken, ValueTask<TNext>> step,
        ParallelStepOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(step);

        var effectiveOptions = options ?? ParallelStepOptions.Default();

        return new PipelineBuilder<TInput, IReadOnlyList<TNext>>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                if (currentValue is not IEnumerable<TItem> sourceItems)
                    throw new InvalidOperationException(
                        $"{nameof(ThenParallel)} requires the current value to implement {nameof(IEnumerable<>)}.");

                var items = sourceItems as IList<TItem> ?? [.. sourceItems];
                if (items.Count == 0)
                    return [];

                var results = new TNext[items.Count];
                var parallelOptions = new ParallelOptions
                {
                    CancellationToken = cancellationToken,
                    MaxDegreeOfParallelism = effectiveOptions.MaxDegreeOfParallelism
                };

                await Parallel.ForEachAsync(
                        Enumerable.Range(0, items.Count),
                        parallelOptions,
                        async (index, token) =>
                        {
                            results[index] = await step(items[index], token).ConfigureAwait(false);
                        })
                    .ConfigureAwait(false);

                return results;
            },
            logger,
            graph.AddStep(stepName, typeof(TCurrent), typeof(IReadOnlyList<TNext>), PipelineNodeKind.Parallel));
    }

    /// <summary>
    /// Adds an execution policy around the existing chain.
    /// </summary>
    /// <param name="policy">The policy to apply.</param>
    /// <returns>The current builder with wrapped execution.</returns>
    public PipelineBuilder<TInput, TCurrent> WithPolicy(IPipelineExecutionPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);

        return new PipelineBuilder<TInput, TCurrent>(
            (input, cancellationToken) => policy.ExecuteAsync(token => chain(input, token), cancellationToken),
            logger,
            graph.AddStep(policy.GetType().Name, typeof(TCurrent), typeof(TCurrent), PipelineNodeKind.Policy));
    }

    private PipelineBuilder<TInput, TNext> ThenAsyncCore<TNext>(
        string? stepName,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options)
    {
        ArgumentNullException.ThrowIfNull(step);

        var effectiveOptions = options ?? StepExecutionOptions.None();
        var effectiveStepName = stepName ?? effectiveOptions.Name;
        var concurrencyGate = effectiveOptions.MaxConcurrency is { } maxConcurrency
            ? new SemaphoreSlim(maxConcurrency, maxConcurrency)
            : null;

        return new PipelineBuilder<TInput, TNext>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                return await ExecuteWithConcurrencyAsync(currentValue, cancellationToken).ConfigureAwait(false);
            },
            logger,
            graph.AddStep(effectiveStepName, typeof(TCurrent), typeof(TNext), PipelineNodeKind.Step));

        async ValueTask<TNext> ExecuteUserStepAsync(TCurrent currentValue, CancellationToken cancellationToken) =>
            await step(currentValue, cancellationToken).ConfigureAwait(false);

        async ValueTask<TNext> ExecuteWithPolicyAsync(TCurrent currentValue, CancellationToken cancellationToken)
        {
            if (effectiveOptions.Policy is { } policy)
                return await policy.ExecuteAsync(token => ExecuteUserStepAsync(currentValue, token), cancellationToken)
                    .ConfigureAwait(false);

            return await ExecuteUserStepAsync(currentValue, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithRateLimitAsync(TCurrent currentValue, CancellationToken cancellationToken)
        {
            if (effectiveOptions.RateLimiter is not { } rateLimiter)
                return await ExecuteWithPolicyAsync(currentValue, cancellationToken).ConfigureAwait(false);

            using var lease = await rateLimiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
            if (!lease.IsAcquired)
                throw new PipelineRateLimitRejectedException(effectiveStepName ?? "Unnamed step");

            return await ExecuteWithPolicyAsync(currentValue, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithConcurrencyAsync(TCurrent currentValue, CancellationToken cancellationToken)
        {
            if (concurrencyGate is null)
                return await ExecuteWithRateLimitAsync(currentValue, cancellationToken).ConfigureAwait(false);

            await concurrencyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await ExecuteWithRateLimitAsync(currentValue, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                concurrencyGate.Release();
            }
        }
    }
}
