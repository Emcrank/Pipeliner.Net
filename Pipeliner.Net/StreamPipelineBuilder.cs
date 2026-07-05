using System;
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
    private readonly Func<TInput, CancellationToken, ValueTask<TCurrent>> chain;
    private readonly PipelineGraph graph;
    private readonly ILogger? logger;
    private readonly BackpressureOptions options;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamPipelineBuilder{TInput,TCurrent}" /> class.
    /// </summary>
    /// <param name="chain">The stream execution chain.</param>
    /// <param name="logger">The optional logger used by built stream pipelines.</param>
    /// <param name="options">The backpressure options for stream execution.</param>
    /// <param name="graph">The pipeline definition graph.</param>
    internal StreamPipelineBuilder(
        Func<TInput, CancellationToken, ValueTask<TCurrent>> chain,
        ILogger? logger,
        BackpressureOptions options,
        PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(graph);

        this.chain = chain;
        this.logger = logger;
        this.options = options;
        this.graph = graph;
    }

    /// <summary>
    /// Builds a stream pipeline from the current chain.
    /// </summary>
    /// <param name="pipelineName">The optional pipeline name.</param>
    /// <returns>The built stream pipeline.</returns>
    public StreamPipeline<TInput, TCurrent> Build(string? pipelineName = null)
    {
        var pipeline = new StreamPipeline<TInput, TCurrent>(chain, logger, options)
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
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                return step(currentValue);
            },
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
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                return await step(currentValue, cancellationToken).ConfigureAwait(false);
            },
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

        return new StreamPipelineBuilder<TInput, TCurrent>(chain, logger, backpressureOptions, graph);
    }
}
