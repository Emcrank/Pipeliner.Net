using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Pipeliner.Net;

/// <summary>
/// Entry point for creating type-threaded pipelines.
/// </summary>
public static class Pipeline
{
    /// <summary>
    /// Creates a builder starting with <typeparamref name="TInput" /> as both input and current output type.
    /// </summary>
    /// <typeparam name="TInput">The initial input type.</typeparam>
    /// <param name="logger">Optional logger used by the built pipeline.</param>
    /// <returns>A type-threaded builder instance.</returns>
    public static PipelineBuilder<TInput, TInput> For<TInput>(ILogger? logger = null) =>
        new((input, _) => ValueTask.FromResult(input), logger);

    /// <summary>
    /// Creates a channel-backed stream builder starting with <typeparamref name="TInput" /> as both input and current output
    /// type.
    /// </summary>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="logger">Optional logger used by the built stream pipeline.</param>
    /// <returns>A stream pipeline builder instance.</returns>
    public static StreamPipelineBuilder<TInput, TInput> StreamFor<TInput>(ILogger? logger = null) =>
        new((input, _) => ValueTask.FromResult(input), logger, BackpressureOptions.Default());
}