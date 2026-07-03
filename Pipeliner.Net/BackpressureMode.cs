namespace Pipeliner.Net;

/// <summary>
/// Defines bounded channel behavior when capacity is reached.
/// </summary>
public enum BackpressureMode
{
    /// <summary>
    /// Waits until space is available.
    /// </summary>
    Wait,

    /// <summary>
    /// Drops the newest item already in the channel to make space.
    /// </summary>
    DropNewest,

    /// <summary>
    /// Drops the oldest item in the channel to make space.
    /// </summary>
    DropOldest,

    /// <summary>
    /// Drops the item being written.
    /// </summary>
    DropWrite
}