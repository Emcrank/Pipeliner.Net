using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Pipeliner.Net;

/// <summary>
/// Represents a channel-backed streaming pipeline with backpressure-aware execution.
/// </summary>
/// <typeparam name="TInput">The stream input item type.</typeparam>
/// <typeparam name="TOutput">The stream output item type.</typeparam>
public sealed class StreamPipeline<TInput, TOutput>
{
    private readonly Func<IAsyncEnumerable<TInput>, CancellationToken, IAsyncEnumerable<TOutput>> transform;
    private PipelineDefinition? definition;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamPipeline{TInput,TOutput}" /> class.
    /// </summary>
    /// <param name="transform">The stream execution transform.</param>
    /// <param name="logger">The optional logger used by stream execution.</param>
    /// <param name="backpressureOptions">The backpressure options.</param>
    internal StreamPipeline(
        Func<IAsyncEnumerable<TInput>, CancellationToken, IAsyncEnumerable<TOutput>> transform,
        ILogger? logger,
        BackpressureOptions backpressureOptions)
    {
        ArgumentNullException.ThrowIfNull(transform);
        ArgumentNullException.ThrowIfNull(backpressureOptions);

        this.transform = transform;
        Logger = logger;
        BackpressureOptions = backpressureOptions;
    }

    /// <summary>
    /// Gets or sets the unique stream pipeline identifier.
    /// </summary>
    public string Id { get; internal set; } = Guid.NewGuid().ToString("D");

    /// <summary>
    /// Gets or sets the friendly stream pipeline name.
    /// </summary>
    public string Name { get; internal set; } = "Unnamed_Stream_Pipeline";

    /// <summary>
    /// Gets a structural description of this stream pipeline.
    /// </summary>
    /// <returns>The pipeline definition graph.</returns>
    public PipelineDefinition Describe() => definition ??= PipelineGraph
        .Create(typeof(TInput))
        .AddStep("Result", typeof(TInput), typeof(TOutput), PipelineNodeKind.Step)
        .ToDefinition(Id, Name);

    /// <summary>
    /// Gets the configured backpressure options.
    /// </summary>
    public BackpressureOptions BackpressureOptions { get; }

    /// <summary>
    /// Gets the logger used by this stream pipeline.
    /// </summary>
    private ILogger? Logger { get; }

    /// <summary>
    /// Runs the pipeline over an asynchronous source stream.
    /// </summary>
    /// <param name="source">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An asynchronous stream of transformed items.</returns>
    public async IAsyncEnumerable<TOutput> RunStreamAsync(
        IAsyncEnumerable<TInput> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);

        var channel = Channel.CreateBounded<TInput>(
            new BoundedChannelOptions(BackpressureOptions.Capacity)
            {
                FullMode = BackpressureOptions.Mode.ToChannelMode(),
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = false
            });

        var producerTask = ProduceAsync(source, channel.Writer, cancellationToken);

        try
        {
            var transformedItems = transform(channel.Reader.ReadAllAsync(cancellationToken), cancellationToken);
            await foreach (var item in transformedItems.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }

            await AwaitProducerCompletionAsync(producerTask, cancellationToken, suppressCancellationException: false).ConfigureAwait(false);
        }
        finally
        {
            channel.Writer.TryComplete();
            await AwaitProducerCompletionAsync(producerTask, cancellationToken, suppressCancellationException: true).ConfigureAwait(false);
        }
    }

    internal void SetDefinition(PipelineDefinition pipelineDefinition)
    {
        ArgumentNullException.ThrowIfNull(pipelineDefinition);
        definition = pipelineDefinition;
    }

    private static async Task AwaitProducerCompletionAsync(
        Task producerTask,
        CancellationToken cancellationToken,
        bool suppressCancellationException)
    {
        try
        {
            await producerTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested && suppressCancellationException)
        { }
        catch (AggregateException exception) when (cancellationToken.IsCancellationRequested &&
                                                  exception.InnerExceptions.All(innerException => innerException is OperationCanceledException))
        {
            if (!suppressCancellationException)
                throw new OperationCanceledException(cancellationToken);
        }
    }

    private async Task ProduceAsync(
        IAsyncEnumerable<TInput> source,
        ChannelWriter<TInput> writer,
        CancellationToken cancellationToken)
    {
        Exception? writeException = null;

        try
        {
            await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            writeException = exception;
            Logger?.LogError(exception, "Stream pipeline {PipelineName} producer failed.", Name);
            throw;
        }
        finally
        {
            writer.TryComplete(writeException);
        }
    }
}