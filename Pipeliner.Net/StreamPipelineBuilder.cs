using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Pipeliner.Net;

/// <summary>
/// Represents a type-threaded builder for channel-backed streaming pipelines.
/// </summary>
/// <typeparam name="TInput">The original stream item input type.</typeparam>
/// <typeparam name="TCurrent">The current stream item type after applied steps.</typeparam>
public sealed class StreamPipelineBuilder<TInput, TCurrent>
{
    private readonly Func<IAsyncEnumerable<TInput>, CancellationToken, IAsyncEnumerable<TCurrent>> transform;
    private readonly PipelineGraph graph;
    private readonly ILogger? logger;
    private readonly BackpressureOptions options;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamPipelineBuilder{TInput,TCurrent}" /> class.
    /// </summary>
    /// <param name="transform">The stream execution transform.</param>
    /// <param name="logger">The optional logger used by built stream pipelines.</param>
    /// <param name="options">The backpressure options for stream execution.</param>
    /// <param name="graph">The pipeline definition graph.</param>
    internal StreamPipelineBuilder(
        Func<IAsyncEnumerable<TInput>, CancellationToken, IAsyncEnumerable<TCurrent>> transform,
        ILogger? logger,
        BackpressureOptions options,
        PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(transform);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(graph);

        this.transform = transform;
        this.logger = logger;
        this.options = options;
        this.graph = graph;
    }

    /// <summary>
    /// Groups stream items into batches by count and optional maximum delay.
    /// </summary>
    /// <param name="size">The maximum number of items per batch.</param>
    /// <param name="maxDelay">The optional maximum amount of time to wait before flushing a partial batch.</param>
    /// <returns>A new stream builder whose current item is a batch.</returns>
    public StreamPipelineBuilder<TInput, IReadOnlyList<TCurrent>> Batch(int size, TimeSpan? maxDelay = null)
    {
        if (size <= 0)
            throw new ArgumentOutOfRangeException(nameof(size), "Batch size must be greater than zero.");

        if (maxDelay is { } delay && delay <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(maxDelay), "Max delay must be greater than zero.");

        return new StreamPipelineBuilder<TInput, IReadOnlyList<TCurrent>>(
            (source, cancellationToken) => BatchAsync(transform(source, cancellationToken), size, maxDelay, cancellationToken),
            logger,
            options,
            graph.AddStep("Batch", typeof(TCurrent), typeof(IReadOnlyList<TCurrent>), PipelineNodeKind.Batch));
    }

    /// <summary>
    /// Builds a stream pipeline from the current transform.
    /// </summary>
    /// <param name="pipelineName">The optional pipeline name.</param>
    /// <returns>The built stream pipeline.</returns>
    public StreamPipeline<TInput, TCurrent> Build(string? pipelineName = null)
    {
        var pipeline = new StreamPipeline<TInput, TCurrent>(transform, logger, options)
        {
            Name = string.IsNullOrWhiteSpace(pipelineName) ? "Unnamed_Stream_Pipeline" : pipelineName
        };

        pipeline.SetDefinition(graph.ToDefinition(pipeline.Id, pipeline.Name));
        return pipeline;
    }

    /// <summary>
    /// Appends a synchronous stream step.
    /// </summary>
    /// <typeparam name="TNext">The next stream item type.</typeparam>
    /// <param name="step">The synchronous stream step.</param>
    /// <returns>A new stream builder with the updated output type.</returns>
    public StreamPipelineBuilder<TInput, TNext> Then<TNext>(Func<TCurrent, TNext> step) =>
        Then(null, step);

    /// <summary>
    /// Appends a synchronous stream step.
    /// </summary>
    /// <typeparam name="TNext">The next stream item type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The synchronous stream step.</param>
    /// <returns>A new stream builder with the updated output type.</returns>
    public StreamPipelineBuilder<TInput, TNext> Then<TNext>(string? stepName, Func<TCurrent, TNext> step)
    {
        ArgumentNullException.ThrowIfNull(step);

        return new StreamPipelineBuilder<TInput, TNext>(
            (source, cancellationToken) => MapAsync(transform(source, cancellationToken), step, cancellationToken),
            logger,
            options,
            graph.AddStep(stepName, typeof(TCurrent), typeof(TNext), PipelineNodeKind.Step));
    }

    /// <summary>
    /// Appends an asynchronous stream step.
    /// </summary>
    /// <typeparam name="TNext">The next stream item type.</typeparam>
    /// <param name="step">The asynchronous stream step.</param>
    /// <returns>A new stream builder with the updated output type.</returns>
    public StreamPipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        Func<TCurrent, CancellationToken, ValueTask<TNext>> step) =>
        ThenAsync(null, step);

    /// <summary>
    /// Appends an asynchronous stream step.
    /// </summary>
    /// <typeparam name="TNext">The next stream item type.</typeparam>
    /// <param name="stepName">The step display name used in pipeline descriptions.</param>
    /// <param name="step">The asynchronous stream step.</param>
    /// <returns>A new stream builder with the updated output type.</returns>
    public StreamPipelineBuilder<TInput, TNext> ThenAsync<TNext>(
        string? stepName,
        Func<TCurrent, CancellationToken, ValueTask<TNext>> step)
    {
        ArgumentNullException.ThrowIfNull(step);

        return new StreamPipelineBuilder<TInput, TNext>(
            (source, cancellationToken) => MapAsync(transform(source, cancellationToken), step, cancellationToken),
            logger,
            options,
            graph.AddStep(stepName, typeof(TCurrent), typeof(TNext), PipelineNodeKind.Step));
    }

    /// <summary>
    /// Configures channel backpressure behavior.
    /// </summary>
    /// <param name="backpressureOptions">The backpressure options.</param>
    /// <returns>A new stream builder with updated backpressure settings.</returns>
    public StreamPipelineBuilder<TInput, TCurrent> WithBackpressure(BackpressureOptions backpressureOptions)
    {
        ArgumentNullException.ThrowIfNull(backpressureOptions);

        if (backpressureOptions.Capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(backpressureOptions), "Capacity must be greater than zero.");

        return new StreamPipelineBuilder<TInput, TCurrent>(transform, logger, backpressureOptions, graph);
    }

    /// <summary>
    /// Groups stream items into time-based windows.
    /// </summary>
    /// <param name="duration">The maximum window duration.</param>
    /// <returns>A new stream builder whose current item is a window.</returns>
    public StreamPipelineBuilder<TInput, IReadOnlyList<TCurrent>> Window(TimeSpan duration)
    {
        if (duration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(duration), "Window duration must be greater than zero.");

        return new StreamPipelineBuilder<TInput, IReadOnlyList<TCurrent>>(
            (source, cancellationToken) => BatchAsync(transform(source, cancellationToken), null, duration, cancellationToken),
            logger,
            options,
            graph.AddStep("Window", typeof(TCurrent), typeof(IReadOnlyList<TCurrent>), PipelineNodeKind.Window));
    }

    private static async IAsyncEnumerable<IReadOnlyList<TItem>> BatchAsync<TItem>(
        IAsyncEnumerable<TItem> source,
        int? size,
        TimeSpan? maxDelay,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batch = new List<TItem>();
        CancellationTokenSource? delayCancellationTokenSource = null;
        Task? delayTask = null;

        await using var enumerator = source.GetAsyncEnumerator(cancellationToken);
        var moveNextTask = enumerator.MoveNextAsync().AsTask();

        try
        {
            while (true)
            {
                if (delayTask is null)
                {
                    if (!await moveNextTask.ConfigureAwait(false))
                        break;

                    AddCurrentItem();
                    if (IsBatchFull())
                    {
                        StopDelay(ref delayCancellationTokenSource, ref delayTask);
                        yield return Flush(batch);
                    }

                    moveNextTask = enumerator.MoveNextAsync().AsTask();
                    continue;
                }

                var completedTask = await Task.WhenAny(moveNextTask, delayTask).ConfigureAwait(false);
                if (completedTask == delayTask)
                {
                    await delayTask.ConfigureAwait(false);

                    if (batch.Count > 0)
                        yield return Flush(batch);

                    StopDelay(ref delayCancellationTokenSource, ref delayTask);
                    continue;
                }

                if (!await moveNextTask.ConfigureAwait(false))
                    break;

                AddCurrentItem();
                if (IsBatchFull())
                {
                    StopDelay(ref delayCancellationTokenSource, ref delayTask);
                    yield return Flush(batch);
                }

                moveNextTask = enumerator.MoveNextAsync().AsTask();
            }
        }
        finally
        {
            StopDelay(ref delayCancellationTokenSource, ref delayTask);
        }

        if (batch.Count > 0)
            yield return Flush(batch);

        void AddCurrentItem()
        {
            cancellationToken.ThrowIfCancellationRequested();
            batch.Add(enumerator.Current);

            if (maxDelay.HasValue && batch.Count == 1)
            {
                StopDelay(ref delayCancellationTokenSource, ref delayTask);
                delayCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                delayTask = Task.Delay(maxDelay.Value, delayCancellationTokenSource.Token);
            }
        }

        bool IsBatchFull() => size is { } maxSize && batch.Count >= maxSize;
    }

    private static IReadOnlyList<TItem> Flush<TItem>(List<TItem> batch)
    {
        var result = batch.ToArray();
        batch.Clear();
        return result;
    }

    private static async IAsyncEnumerable<TNext> MapAsync<TSource, TNext>(
        IAsyncEnumerable<TSource> source,
        Func<TSource, TNext> step,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            yield return step(item);
    }

    private static async IAsyncEnumerable<TNext> MapAsync<TSource, TNext>(
        IAsyncEnumerable<TSource> source,
        Func<TSource, CancellationToken, ValueTask<TNext>> step,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            yield return await step(item, cancellationToken).ConfigureAwait(false);
    }

    private static void StopDelay(ref CancellationTokenSource? cancellationTokenSource, ref Task? delayTask)
    {
        cancellationTokenSource?.Cancel();
        cancellationTokenSource?.Dispose();
        cancellationTokenSource = null;
        delayTask = null;
    }
}