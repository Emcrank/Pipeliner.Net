using System.Threading.Channels;

namespace Pipeliner.Net;

internal static class BackpressureModeMapper
{
    /// <summary>
    /// Converts a <see cref="BackpressureMode" /> to <see cref="BoundedChannelFullMode" />.
    /// </summary>
    /// <param name="mode">The backpressure mode.</param>
    /// <returns>The mapped channel full mode.</returns>
    public static BoundedChannelFullMode ToChannelMode(this BackpressureMode mode) =>
        mode switch
        {
            BackpressureMode.Wait => BoundedChannelFullMode.Wait,
            BackpressureMode.DropNewest => BoundedChannelFullMode.DropNewest,
            BackpressureMode.DropOldest => BoundedChannelFullMode.DropOldest,
            BackpressureMode.DropWrite => BoundedChannelFullMode.DropWrite,
            _ => BoundedChannelFullMode.Wait
        };
}