using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Represents a typed pipeline step.
/// </summary>
/// <typeparam name="TInput">The input type.</typeparam>
/// <typeparam name="TOutput">The output type.</typeparam>
public interface IPipelineStep<in TInput, TOutput>
{
    /// <summary>
    /// Executes the step.
    /// </summary>
    /// <param name="input">The step input.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The produced output.</returns>
    ValueTask<TOutput> ExecuteAsync(TInput input, CancellationToken cancellationToken = default);
}