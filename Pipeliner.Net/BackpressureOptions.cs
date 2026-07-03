using System;

namespace Pipeliner.Net;

/// <summary>
/// Configures backpressure behavior for streaming pipelines.
/// </summary>
/// <param name="Capacity">The bounded channel capacity.</param>
/// <param name="Mode">The channel full behavior when capacity is reached.</param>
public sealed record BackpressureOptions(int Capacity, BackpressureMode Mode)
{
    /// <summary>
    /// Creates a validated options instance.
    /// </summary>
    /// <param name="capacity">The bounded channel capacity.</param>
    /// <param name="mode">The channel full behavior.</param>
    /// <returns>A validated options instance.</returns>
    public static BackpressureOptions Create(int capacity, BackpressureMode mode = BackpressureMode.Wait)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than zero.");

        return new BackpressureOptions(capacity, mode);
    }

    /// <summary>
    /// Creates default backpressure options.
    /// </summary>
    /// <returns>A default options instance.</returns>
    public static BackpressureOptions Default() => new(256, BackpressureMode.Wait);
}