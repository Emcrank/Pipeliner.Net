using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Provides built-in reducers for fork/merge orchestration.
/// </summary>
public static class MergeReducers
{
    /// <summary>
    /// Returns only successful branch results.
    /// </summary>
    /// <typeparam name="T">The branch output type.</typeparam>
    /// <param name="results">The fork branch results.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list containing all successful values.</returns>
    public static ValueTask<IReadOnlyList<T>> IgnoreFailuresAsync<T>(
        IReadOnlyList<ForkResult<T>> results,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(results);
        cancellationToken.ThrowIfCancellationRequested();

        IReadOnlyList<T> successfulResults = results.Where(result => result.IsSuccess)
            .Select(result => result.Value!)
            .ToArray();

        return ValueTask.FromResult(successfulResults);
    }

    /// <summary>
    /// Aggregates successful branch values with an asynchronous accumulator.
    /// </summary>
    /// <typeparam name="T">The branch output type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="results">The fork branch results.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The asynchronous accumulator delegate.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The accumulated value.</returns>
    public static async ValueTask<TAccumulate> ReduceAsync<T, TAccumulate>(
        IReadOnlyList<ForkResult<T>> results,
        TAccumulate seed,
        Func<TAccumulate, T, CancellationToken, ValueTask<TAccumulate>> accumulator,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(results);
        ArgumentNullException.ThrowIfNull(accumulator);

        var current = seed;

        foreach (var result in results)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!result.IsSuccess)
                continue;

            current = await accumulator(current, result.Value!, cancellationToken).ConfigureAwait(false);
        }

        return current;
    }

    /// <summary>
    /// Returns the first successful branch value.
    /// </summary>
    /// <typeparam name="T">The branch output type.</typeparam>
    /// <param name="results">The fork branch results.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The first successful value.</returns>
    public static ValueTask<T> TakeFirstAsync<T>(
        IReadOnlyList<ForkResult<T>> results,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(results);
        cancellationToken.ThrowIfCancellationRequested();

        var firstResult = results.FirstOrDefault(result => result.IsSuccess);
        if (firstResult is { IsSuccess: true, Value: { } value })
            return ValueTask.FromResult(value);

        var failures = results.Where(result => !result.IsSuccess)
            .Select(result => result.Error!)
            .ToArray();

        if (failures.Length > 0)
            throw new AggregateException(failures);

        throw new InvalidOperationException("No successful fork results were produced.");
    }

    /// <summary>
    /// Throws when any branch failed, otherwise returns successful results.
    /// </summary>
    /// <typeparam name="T">The branch output type.</typeparam>
    /// <param name="results">The fork branch results.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list containing all successful values.</returns>
    public static ValueTask<IReadOnlyList<T>> ThrowOnAnyFailureAsync<T>(
        IReadOnlyList<ForkResult<T>> results,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(results);
        cancellationToken.ThrowIfCancellationRequested();

        var failures = results.Where(result => !result.IsSuccess)
            .Select(result => result.Error!)
            .ToArray();

        if (failures.Length > 0)
            throw new AggregateException(failures);

        IReadOnlyList<T> successfulResults = results.Where(result => result.IsSuccess)
            .Select(result => result.Value!)
            .ToArray();

        return ValueTask.FromResult(successfulResults);
    }
}