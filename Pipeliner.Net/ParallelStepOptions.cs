using System;

namespace Pipeliner.Net;

/// <summary>
/// Configures parallel step behavior.
/// </summary>
/// <param name="MaxDegreeOfParallelism">The max degree of parallelism. Must be at least 1.</param>
public sealed record ParallelStepOptions(int MaxDegreeOfParallelism)
{
    /// <summary>
    /// Creates default parallel options that use <see cref="Environment.ProcessorCount"/> workers.
    /// </summary>
    /// <returns>A default options instance.</returns>
    public static ParallelStepOptions Default() => new(Math.Max(1, Environment.ProcessorCount));

    /// <summary>
    /// Creates options with a specific max degree of parallelism.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The max degree of parallelism. Must be at least 1.</param>
    /// <returns>A configured options instance.</returns>
    public static ParallelStepOptions Create(int maxDegreeOfParallelism) =>
        new(maxDegreeOfParallelism < 1 ? throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism)) : maxDegreeOfParallelism);
}