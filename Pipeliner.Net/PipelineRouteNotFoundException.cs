using System;

namespace Pipeliner.Net;

/// <summary>
/// The exception thrown when no dynamic route matches and no default route is configured.
/// </summary>
public sealed class PipelineRouteNotFoundException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PipelineRouteNotFoundException" /> class.
    /// </summary>
    /// <param name="routeKey">The unmatched route key.</param>
    public PipelineRouteNotFoundException(object? routeKey)
        : base($"No pipeline route matched key `{routeKey}` and no default route was configured.")
    {
        RouteKey = routeKey;
    }

    /// <summary>
    /// Gets the unmatched route key.
    /// </summary>
    public object? RouteKey { get; }
}