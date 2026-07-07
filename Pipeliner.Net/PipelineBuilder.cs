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
    private readonly PipelineCheckpointOptions? checkpointOptions;
    private readonly PipelineExecutablePlan<TInput, TCurrent> plan;
    private readonly PipelineGraph graph;
    private readonly ILogger? logger;
    private readonly IReadOnlyList<IPipelineObserver> observers;

    /// <summary>
    /// Initializes a new instance of <see cref="PipelineBuilder{TInput,TCurrent}" />.
    /// </summary>
    /// <param name="chain">The pipeline execution chain.</param>
    /// <param name="logger">The optional logger used by built pipelines.</param>
    /// <param name="graph">The pipeline definition graph.</param>
    /// <param name="plan">The optional executable pipeline plan.</param>
    /// <param name="checkpointOptions">The optional checkpoint persistence options.</param>
    /// <param name="observers">The optional pipeline observers.</param>
    internal PipelineBuilder(
        Func<TInput, CancellationToken, ValueTask<TCurrent>> chain,
        ILogger? logger,
        PipelineGraph graph,
        PipelineExecutablePlan<TInput, TCurrent>? plan = null,
        PipelineCheckpointOptions? checkpointOptions = null,
        IReadOnlyList<IPipelineObserver>? observers = null)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(graph);

        this.chain = chain;
        this.logger = logger;
        this.graph = graph;
        this.plan = plan ?? PipelineExecutablePlan<TInput, TInput>
            .Create()
            .Then(
                graph.TerminalNode.Id,
                graph.TerminalNode.Name,
                graph.TerminalNode.Kind,
                async (input, _, cancellationToken) => await chain(input, cancellationToken).ConfigureAwait(false));
        this.checkpointOptions = checkpointOptions;
        this.observers = observers ?? [];
    }

    /// <summary>
    /// Routes execution to a keyed route handler based on runtime data.
    /// </summary>
    /// <typeparam name="TKey">The route key type.</typeparam>
    /// <typeparam name="TNext">The route output type.</typeparam>
    /// <param name="routeSelector">The route key selector.</param>
    /// <param name="configureRoutes">The route configuration callback.</param>
    /// <returns>A new builder with the route output type.</returns>
    public PipelineBuilder<TInput, TNext> RouteBy<TKey, TNext>(
        Func<TCurrent, TKey> routeSelector,
        Action<PipelineRouteBuilder<TCurrent, TKey, TNext>> configureRoutes)
        where TKey : notnull =>
        RouteBy(null, routeSelector, configureRoutes);

    /// <summary>
    /// Routes execution to a keyed route handler based on runtime data.
    /// </summary>
    /// <typeparam name="TKey">The route key type.</typeparam>
    /// <typeparam name="TNext">The route output type.</typeparam>
    /// <param name="stepName">The route display name used in pipeline descriptions.</param>
    /// <param name="routeSelector">The route key selector.</param>
    /// <param name="configureRoutes">The route configuration callback.</param>
    /// <returns>A new builder with the route output type.</returns>
    public PipelineBuilder<TInput, TNext> RouteBy<TKey, TNext>(
        string? stepName,
        Func<TCurrent, TKey> routeSelector,
        Action<PipelineRouteBuilder<TCurrent, TKey, TNext>> configureRoutes)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(routeSelector);
        ArgumentNullException.ThrowIfNull(configureRoutes);

        var routeBuilder = new PipelineRouteBuilder<TCurrent, TKey, TNext>();
        configureRoutes(routeBuilder);

        if (routeBuilder.Routes.Count == 0 && routeBuilder.DefaultRoute is null)
            throw new ArgumentException("At least one route or default route must be configured.", nameof(configureRoutes));

        return new PipelineBuilder<TInput, TNext>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                var traceContext = PipelineTraceContext.Current;
                if (traceContext is null)
                    return await ExecuteRouteAsync(currentValue, cancellationToken).ConfigureAwait(false);

                return await traceContext.TraceStepAsync(
                    stepName ?? PipelineNodeKind.Route.ToString(),
                    PipelineNodeKind.Route,
                    typeof(TCurrent),
                    typeof(TNext),
                    () => ExecuteRouteAsync(currentValue, cancellationToken)).ConfigureAwait(false);
            },
            logger,
            graph.AddStep(stepName, typeof(TCurrent), typeof(TNext), PipelineNodeKind.Route),
            null,
            checkpointOptions, observers);

        async ValueTask<TNext> ExecuteRouteAsync(TCurrent currentValue, CancellationToken cancellationToken)
        {
            var routeKey = routeSelector(currentValue);

            if (routeBuilder.Routes.TryGetValue(routeKey, out var route))
                return await route(currentValue, cancellationToken).ConfigureAwait(false);

            if (routeBuilder.DefaultRoute is { } defaultRoute)
                return await defaultRoute(currentValue, cancellationToken).ConfigureAwait(false);

            throw new PipelineRouteNotFoundException(routeKey);
        }
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
            graph.AddBranch(stepName, typeof(TCurrent), typeof(TNext)),
            null,
            checkpointOptions, observers);
    }

    /// <summary>
    /// Builds an <see cref="OperationPipeline{TParam, TResult}" /> from the current chain.
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
                        sagaToken => plan.ExecuteAsync(input, executionContext, sagaToken),
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
                branches.Length),
            null,
            checkpointOptions, observers);

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
            graph.AddStep(stepName, typeof(ForkExecutionResult<TBranch>), typeof(TNext), PipelineNodeKind.Merge),
            null,
            checkpointOptions, observers);
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
    /// Appends a compensatable saga step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="execute">The step execution delegate.</param>
    /// <param name="compensate">The compensation callback registered after successful execution.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenSaga<TNext>(
        Func<TCurrent, CancellationToken, ValueTask<TNext>> execute,
        Func<TNext, CancellationToken, ValueTask> compensate,
        StepExecutionOptions? options = null) =>
        ThenSaga(null, execute, compensate, options);

    /// <summary>
    /// Appends a compensatable saga step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="execute">The step execution delegate.</param>
    /// <param name="compensate">The compensation callback registered after successful execution.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenSaga<TNext>(
        string? stepName,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> execute,
        Func<TNext, CancellationToken, ValueTask> compensate,
        StepExecutionOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(execute);
        ArgumentNullException.ThrowIfNull(compensate);

        return ThenAsyncCore(
            stepName,
            async (current, _, cancellationToken) =>
            {
                var result = await execute(current, cancellationToken).ConfigureAwait(false);
                PipelineSagaContext.Current?.Register(token => compensate(result, token));
                return result;
            },
            options,
            PipelineNodeKind.Saga);
    }

    /// <summary>
    /// Appends an asynchronous step to the builder.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The asynchronous step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(Func<TCurrent, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsyncCore(null, (current, _, cancellationToken) => step(current, cancellationToken), StepExecutionOptions.None());

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
        ThenAsyncCore(stepName, (current, _, cancellationToken) => step(current, cancellationToken), StepExecutionOptions.None());

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
        ThenAsyncCore(null, (current, _, cancellationToken) => step(current, cancellationToken), options);

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
        ThenAsyncCore(stepName, (current, _, cancellationToken) => step(current, cancellationToken), options);

    /// <summary>
    /// Appends an asynchronous step that can inspect the active execution context.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The asynchronous context-aware step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        Func<TCurrent, PipelineExecutionContext, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsyncCore(null, step, StepExecutionOptions.None());

    /// <summary>
    /// Appends an asynchronous step that can inspect the active execution context.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The asynchronous context-aware step delegate.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        string? stepName,
        Func<TCurrent, PipelineExecutionContext, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsyncCore(stepName, step, StepExecutionOptions.None());

    /// <summary>
    /// Appends an asynchronous step that can inspect the active execution context.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="step">The asynchronous context-aware step delegate.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        Func<TCurrent, PipelineExecutionContext, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options) =>
        ThenAsyncCore(null, step, options);

    /// <summary>
    /// Appends an asynchronous step that can inspect the active execution context.
    /// </summary>
    /// <typeparam name="TNext">The next output type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The asynchronous context-aware step delegate.</param>
    /// <param name="options">The step execution options.</param>
    /// <returns>A new builder with updated output type.</returns>
    public PipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        string? stepName,
        Func<TCurrent, PipelineExecutionContext, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options) =>
        ThenAsyncCore(stepName, step, options);

    /// <summary>
    /// Halts the pipeline when the predicate returns true.
    /// </summary>
    /// <param name="predicate">The halt condition.</param>
    /// <returns>A new builder with a halt gate.</returns>
    public PipelineBuilder<TInput, TCurrent> HaltWhen(Func<TCurrent, bool> predicate) =>
        HaltWhen(null, predicate);

    /// <summary>
    /// Halts the pipeline when the predicate returns true.
    /// </summary>
    /// <param name="haltName">The halt display name used in pipeline descriptions and halt events.</param>
    /// <param name="predicate">The halt condition.</param>
    /// <returns>A new builder with a halt gate.</returns>
    public PipelineBuilder<TInput, TCurrent> HaltWhen(string? haltName, Func<TCurrent, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        return ThenAsyncCore(
            haltName,
            (current, context, _) =>
            {
                if (predicate(current))
                    throw new PipelineHaltedException(context.RunId, context.StepName ?? "Halt", context.StepId ?? "halt");

                return ValueTask.FromResult(current);
            },
            StepExecutionOptions.None(),
            PipelineNodeKind.Halt);
    }

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

        return ThenAsyncCore<IReadOnlyList<TNext>>(
            stepName,
            async (currentValue, _, cancellationToken) =>
            {
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
            StepExecutionOptions.None(),
            PipelineNodeKind.Parallel);
    }

    /// <summary>
    /// Adds an observer for run, step, halt, and checkpoint events.
    /// </summary>
    /// <param name="observer">The observer to add.</param>
    /// <returns>A new builder with the observer configured.</returns>
    public PipelineBuilder<TInput, TCurrent> WithObserver(IPipelineObserver observer)
    {
        ArgumentNullException.ThrowIfNull(observer);

        return new PipelineBuilder<TInput, TCurrent>(
            chain,
            logger,
            graph,
            plan,
            checkpointOptions,
            observers.Concat([observer]).ToArray());
    }

    /// <summary>
    /// Configures checkpoint persistence for this request-response pipeline.
    /// </summary>
    /// <param name="options">The checkpoint options.</param>
    /// <returns>A new builder with checkpoint persistence configured.</returns>
    public PipelineBuilder<TInput, TCurrent> WithCheckpointing(PipelineCheckpointOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new PipelineBuilder<TInput, TCurrent>(chain, logger, graph, plan, options, observers);
    }

    /// <summary>
    /// Configures checkpoint persistence for this request-response pipeline.
    /// </summary>
    /// <param name="store">The checkpoint store.</param>
    /// <param name="failureBehavior">The checkpoint persistence failure behavior.</param>
    /// <returns>A new builder with checkpoint persistence configured.</returns>
    public PipelineBuilder<TInput, TCurrent> WithCheckpointing(
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
    public PipelineBuilder<TInput, TCurrent> Checkpoint(string checkpointName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointName);

        var nextGraph = graph.AddStep(checkpointName, typeof(TCurrent), typeof(TCurrent), PipelineNodeKind.Checkpoint);
        var checkpointNode = nextGraph.TerminalNode;

        async ValueTask<TCurrent> SaveAsync(
            TCurrent current,
            PipelineExecutionContext context,
            CancellationToken cancellationToken)
        {
            await context.SaveCheckpointAsync(checkpointName, checkpointNode.Id, current, cancellationToken)
                .ConfigureAwait(false);
            return current;
        }

        var nextPlan = plan.Then(checkpointNode.Id, checkpointNode.Name, checkpointNode.Kind, SaveAsync);

        return new PipelineBuilder<TInput, TCurrent>(
            async (input, cancellationToken) =>
            {
                var current = await chain(input, cancellationToken).ConfigureAwait(false);
                if (PipelineExecutionContext.Current is { } context)
                    return await SaveAsync(current, context, cancellationToken).ConfigureAwait(false);

                return current;
            },
            logger,
            nextGraph,
            nextPlan,
            checkpointOptions,
            observers);
    }

    /// <summary>
    /// Adds per-run state to the pipeline builder.
    /// </summary>
    /// <typeparam name="TState">The per-run state type.</typeparam>
    /// <param name="stateFactory">Factory used to create state for each pipeline run.</param>
    /// <returns>A stateful pipeline builder.</returns>
    public StatefulPipelineBuilder<TInput, TCurrent, TState> WithState<TState>(Func<TState> stateFactory)
    {
        ArgumentNullException.ThrowIfNull(stateFactory);

        return new StatefulPipelineBuilder<TInput, TCurrent, TState>(
            async (input, _, cancellationToken) => await chain(input, cancellationToken).ConfigureAwait(false),
            logger,
            graph,
            stateFactory,
            checkpointOptions,
            observers);
    }

    /// <summary>
    /// Adds an execution policy around the existing chain.
    /// </summary>
    /// <param name="policy">The policy to apply.</param>
    /// <returns>The current builder with wrapped execution.</returns>
    public PipelineBuilder<TInput, TCurrent> WithPolicy(IPipelineExecutionPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);

        var nextGraph = graph.AddStep(policy.GetType().Name, typeof(TCurrent), typeof(TCurrent), PipelineNodeKind.Policy);
        var policyNode = nextGraph.TerminalNode;
        var nextPlan = PipelineExecutablePlan<TInput, TInput>
            .Create()
            .Then(
                policyNode.Id,
                policyNode.Name,
                policyNode.Kind,
                (input, context, cancellationToken) =>
                    policy.ExecuteAsync(token => plan.ExecuteAsync(input, context, token), cancellationToken));

        return new PipelineBuilder<TInput, TCurrent>(
            (input, cancellationToken) => policy.ExecuteAsync(token => chain(input, token), cancellationToken),
            logger,
            nextGraph,
            nextPlan,
            checkpointOptions,
            observers);
    }

    private PipelineBuilder<TInput, TNext> ThenAsyncCore<TNext>(
        string? stepName,
        Func<TCurrent, PipelineExecutionContext, CancellationToken, ValueTask<TNext>> step,
        StepExecutionOptions? options,
        PipelineNodeKind nodeKind = PipelineNodeKind.Step)
    {
        ArgumentNullException.ThrowIfNull(step);

        var effectiveOptions = options ?? StepExecutionOptions.None();
        var effectiveStepName = stepName ?? effectiveOptions.Name;
        var nextGraph = graph.AddStep(effectiveStepName, typeof(TCurrent), typeof(TNext), nodeKind);
        var stepNode = nextGraph.TerminalNode;
        var concurrencyGate = effectiveOptions.MaxConcurrency is { } maxConcurrency
            ? new SemaphoreSlim(maxConcurrency, maxConcurrency)
            : null;

        async ValueTask<TNext> ExecuteStepAsync(
            TCurrent currentValue,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken)
        {
            var traceContext = PipelineTraceContext.Current;
            if (traceContext is null)
                return await ExecuteWithConcurrencyAsync(currentValue, stepContext, cancellationToken).ConfigureAwait(false);

            return await traceContext.TraceStepAsync(
                stepNode.Name,
                stepNode.Kind,
                typeof(TCurrent),
                typeof(TNext),
                () => ExecuteWithConcurrencyAsync(currentValue, stepContext, cancellationToken)).ConfigureAwait(false);
        }

        var nextPlan = plan.Then(stepNode.Id, stepNode.Name, stepNode.Kind, ExecuteStepAsync);

        return new PipelineBuilder<TInput, TNext>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                if (PipelineExecutionContext.Current is { } context)
                    return await ExecuteStepAsync(currentValue, context, cancellationToken).ConfigureAwait(false);

                var fallbackContext = new PipelineExecutionContext(
                    Guid.NewGuid().ToString("D"),
                    "fallback-pipeline",
                    "Fallback Pipeline",
                    "1.0.0",
                    null);
                return await ExecuteStepAsync(currentValue, fallbackContext, cancellationToken).ConfigureAwait(false);
            },
            logger,
            nextGraph,
            nextPlan,
            checkpointOptions,
            observers);

        async ValueTask<TNext> ExecuteUserStepAsync(
            TCurrent currentValue,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken) =>
            await step(currentValue, stepContext, cancellationToken).ConfigureAwait(false);

        async ValueTask<TNext> ExecuteWithPolicyAsync(
            TCurrent currentValue,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken)
        {
            if (effectiveOptions.Policy is { } policy)
                return await policy.ExecuteAsync(
                        token => ExecuteUserStepAsync(currentValue, stepContext, token),
                        cancellationToken)
                    .ConfigureAwait(false);

            return await ExecuteUserStepAsync(currentValue, stepContext, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithRateLimitAsync(
            TCurrent currentValue,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken)
        {
            if (effectiveOptions.RateLimiter is not { } rateLimiter)
                return await ExecuteWithPolicyAsync(currentValue, stepContext, cancellationToken).ConfigureAwait(false);

            using var lease = await rateLimiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
            if (!lease.IsAcquired)
                throw new PipelineRateLimitRejectedException(stepNode.Name);

            return await ExecuteWithPolicyAsync(currentValue, stepContext, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TNext> ExecuteWithConcurrencyAsync(
            TCurrent currentValue,
            PipelineExecutionContext stepContext,
            CancellationToken cancellationToken)
        {
            if (concurrencyGate is null)
                return await ExecuteWithRateLimitAsync(currentValue, stepContext, cancellationToken).ConfigureAwait(false);

            await concurrencyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await ExecuteWithRateLimitAsync(currentValue, stepContext, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                concurrencyGate.Release();
            }
        }
    }
}