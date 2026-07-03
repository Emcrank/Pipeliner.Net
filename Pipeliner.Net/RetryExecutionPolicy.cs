using System;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// A retry policy implementation for pipeline execution.
/// </summary>
/// <param name="maxAttempts">The maximum attempt count.</param>
public sealed class RetryExecutionPolicy(int maxAttempts) : IPipelineExecutionPolicy
{
    private readonly int attempts =
        maxAttempts < 1 ? throw new ArgumentOutOfRangeException(nameof(maxAttempts)) : maxAttempts;

    /// <inheritdoc />
    public async ValueTask<T> ExecuteAsync<T>(Func<CancellationToken, ValueTask<T>> execution,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(execution);

        Exception? lastException = null;

        for (int attempt = 1; attempt <= attempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await execution(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (attempt < attempts)
            {
                lastException = exception;
            }
        }

        throw lastException ?? new InvalidOperationException("Retry policy exhausted without a captured exception.");
    }
}