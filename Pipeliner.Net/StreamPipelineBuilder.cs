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
    private readonly ILogger? logger;
    private readonly BackpressureOptions options;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamPipelineBuilder{TInput,TCurrent}" /> class.
    /// </summary>
    /// <param name="chain">The stream execution chain.</param>
    /// <param name="logger">The optional logger used by built stream pipelines.</param>
    /// <param name="options">The backpressure options for stream execution.</param>
    internal StreamPipelineBuilder(
        Func<TInput, CancellationToken, ValueTask<TCurrent>> chain,
        ILogger? logger,
        BackpressureOptions options)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(options);

        this.chain = chain;
        this.logger = logger;
        this.options = options;
    }

    /// <summary>
    /// Builds a stream pipeline from the current chain.
    /// </summary>
    /// <param name="pipelineName">The optional pipeline name.</param>
    /// <returns>The built stream pipeline.</returns>
    public StreamPipeline<TInput, TCurrent> Build(string? pipelineName = null) =>
        new(chain, logger, options)
        {
            Name = string.IsNullOrWhiteSpace(pipelineName) ? "Unnamed_Stream_Pipeline" : pipelineName
        };

    /// <summary>
    /// Appends a synchronous stream step.
    /// </summary>
    /// <typeparam name="TNext">The next stream item type.</typeparam>
    /// <param name="step">The synchronous stream step.</param>
    /// <returns>A new stream builder with the updated output type.</returns>
    public StreamPipelineBuilder<TInput, TNext> Then<TNext>(Func<TCurrent, TNext> step)
    {
        ArgumentNullException.ThrowIfNull(step);

        return new StreamPipelineBuilder<TInput, TNext>(
            async (input, cancellationToken) =>
            {
                var currentValue = await chain(input, cancellationToken).ConfigureAwait(false);
                return step(currentValue);
            },
            logger,
            options);
    }

    /// <summary>
    /// Appends an asynchronous stream step.
    /// </summary>
    /// <typeparam name="TNext">The next stream item type.</typeparam>
    /// <param name="step">The asynchronous stream step.</param>
    /// <returns>A new stream builder with the updated output type.</returns>
    public StreamPipelineBuilder<TInput, TNext> ThenAsync<TNext>(
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
            options);
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

        return new StreamPipelineBuilder<TInput, TCurrent>(chain, logger, backpressureOptions);
    }
}