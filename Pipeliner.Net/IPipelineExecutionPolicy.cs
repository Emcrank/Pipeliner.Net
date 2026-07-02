using System;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Represents an execution policy that can wrap pipeline execution.
/// </summary>
public interface IPipelineExecutionPolicy
{
    /// <summary>
    /// Executes the provided delegate with policy behavior.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="execution">Delegate representing the next operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The policy-wrapped result.</returns>
    ValueTask<T> ExecuteAsync<T>(Func<CancellationToken, ValueTask<T>> execution, CancellationToken cancellationToken = default);
}