using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

internal sealed class PipelineSagaContext
{
    private static readonly AsyncLocal<PipelineSagaContext?> CurrentContext = new();
    private readonly Stack<Func<CancellationToken, ValueTask>> compensations = new();

    public static PipelineSagaContext? Current => CurrentContext.Value;

    public static async ValueTask<T> RunAsync<T>(
        Func<CancellationToken, ValueTask<T>> execution,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(execution);

        var previousContext = CurrentContext.Value;
        var context = new PipelineSagaContext();
        CurrentContext.Value = context;

        try
        {
            return await execution(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            var compensationFailures = await context.CompensateAsync(cancellationToken).ConfigureAwait(false);
            if (compensationFailures.Count > 0)
                throw new PipelineSagaCompensationException(exception, compensationFailures);

            throw;
        }
        finally
        {
            CurrentContext.Value = previousContext;
        }
    }

    public void Register(Func<CancellationToken, ValueTask> compensation)
    {
        ArgumentNullException.ThrowIfNull(compensation);
        compensations.Push(compensation);
    }

    private async ValueTask<IReadOnlyList<Exception>> CompensateAsync(CancellationToken cancellationToken)
    {
        if (compensations.Count == 0)
            return [];

        var failures = new List<Exception>();

        while (compensations.Count > 0)
        {
            var compensation = compensations.Pop();

            try
            {
                await compensation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }

        return failures;
    }
}