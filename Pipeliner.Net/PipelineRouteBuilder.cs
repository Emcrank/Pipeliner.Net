using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Builds keyed route handlers for a pipeline routing step.
/// </summary>
/// <typeparam name="TInput">The route input type.</typeparam>
/// <typeparam name="TKey">The route key type.</typeparam>
/// <typeparam name="TOutput">The route output type.</typeparam>
public sealed class PipelineRouteBuilder<TInput, TKey, TOutput>
    where TKey : notnull
{
    private readonly Dictionary<TKey, Func<TInput, CancellationToken, ValueTask<TOutput>>> routes = [];
    private Func<TInput, CancellationToken, ValueTask<TOutput>>? defaultRoute;

    internal IReadOnlyDictionary<TKey, Func<TInput, CancellationToken, ValueTask<TOutput>>> Routes => routes;

    internal Func<TInput, CancellationToken, ValueTask<TOutput>>? DefaultRoute => defaultRoute;

    /// <summary>
    /// Adds a synchronous route handler.
    /// </summary>
    /// <param name="key">The route key.</param>
    /// <param name="handler">The route handler.</param>
    /// <returns>The current route builder.</returns>
    public PipelineRouteBuilder<TInput, TKey, TOutput> When(TKey key, Func<TInput, TOutput> handler)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(handler);

        routes[key] = (input, _) => ValueTask.FromResult(handler(input));
        return this;
    }

    /// <summary>
    /// Adds an asynchronous route handler.
    /// </summary>
    /// <param name="key">The route key.</param>
    /// <param name="handler">The route handler.</param>
    /// <returns>The current route builder.</returns>
    public PipelineRouteBuilder<TInput, TKey, TOutput> WhenAsync(
        TKey key,
        Func<TInput, CancellationToken, ValueTask<TOutput>> handler)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(handler);

        routes[key] = handler;
        return this;
    }

    /// <summary>
    /// Adds a synchronous default route handler.
    /// </summary>
    /// <param name="handler">The default route handler.</param>
    /// <returns>The current route builder.</returns>
    public PipelineRouteBuilder<TInput, TKey, TOutput> Default(Func<TInput, TOutput> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        defaultRoute = (input, _) => ValueTask.FromResult(handler(input));
        return this;
    }

    /// <summary>
    /// Adds an asynchronous default route handler.
    /// </summary>
    /// <param name="handler">The default route handler.</param>
    /// <returns>The current route builder.</returns>
    public PipelineRouteBuilder<TInput, TKey, TOutput> DefaultAsync(
        Func<TInput, CancellationToken, ValueTask<TOutput>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        defaultRoute = handler;
        return this;
    }
}