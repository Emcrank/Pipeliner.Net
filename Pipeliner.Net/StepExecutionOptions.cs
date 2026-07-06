using System;
using System.Threading.RateLimiting;

namespace Pipeliner.Net;

/// <summary>
/// Configures per-step execution behavior.
/// </summary>
public sealed record StepExecutionOptions
{
    /// <summary>
    /// Initializes a new instance of <see cref="StepExecutionOptions" />.
    /// </summary>
    /// <param name="policy">Optional policy applied to the step execution.</param>
    /// <param name="maxConcurrency">Optional maximum number of concurrent executions for the step.</param>
    /// <param name="rateLimiter">Optional rate limiter applied to the step.</param>
    /// <param name="name">Optional step display name used in pipeline descriptions.</param>
    public StepExecutionOptions(
        IPipelineExecutionPolicy? policy = null,
        int? maxConcurrency = null,
        RateLimiter? rateLimiter = null,
        string? name = null)
    {
        if (maxConcurrency is < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be at least 1.");

        if (name is not null && string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Step name cannot be empty.", nameof(name));

        Policy = policy;
        MaxConcurrency = maxConcurrency;
        RateLimiter = rateLimiter;
        Name = name;
    }

    /// <summary>
    /// Gets the optional policy applied to the step execution.
    /// </summary>
    public IPipelineExecutionPolicy? Policy { get; init; }

    /// <summary>
    /// Gets the optional maximum number of concurrent executions for the step.
    /// </summary>
    public int? MaxConcurrency { get; init; }

    /// <summary>
    /// Gets the optional rate limiter applied to the step.
    /// </summary>
    public RateLimiter? RateLimiter { get; init; }

    /// <summary>
    /// Gets the optional step display name used in pipeline descriptions.
    /// </summary>
    public string? Name { get; init; }

    /// <summary>
    /// Creates options with no execution policy.
    /// </summary>
    /// <returns>A default options instance.</returns>
    public static StepExecutionOptions None() => new();

    /// <summary>
    /// Creates step execution options.
    /// </summary>
    /// <param name="policy">Optional policy applied to the step execution.</param>
    /// <param name="maxConcurrency">Optional maximum number of concurrent executions for the step.</param>
    /// <param name="rateLimiter">Optional rate limiter applied to the step.</param>
    /// <param name="name">Optional step display name used in pipeline descriptions.</param>
    /// <returns>A configured options instance.</returns>
    public static StepExecutionOptions Create(
        IPipelineExecutionPolicy? policy = null,
        int? maxConcurrency = null,
        RateLimiter? rateLimiter = null,
        string? name = null) =>
        new(policy, maxConcurrency, rateLimiter, name);

    /// <summary>
    /// Creates options with a policy.
    /// </summary>
    /// <param name="policy">The policy to apply.</param>
    /// <returns>A configured options instance.</returns>
    public static StepExecutionOptions WithPolicy(IPipelineExecutionPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        return new StepExecutionOptions(policy);
    }

    /// <summary>
    /// Creates options with a maximum concurrency limit.
    /// </summary>
    /// <param name="maxConcurrency">The maximum number of concurrent step executions.</param>
    /// <returns>A configured options instance.</returns>
    public static StepExecutionOptions WithMaxConcurrency(int maxConcurrency) => new(maxConcurrency: maxConcurrency);

    /// <summary>
    /// Creates options with a rate limiter.
    /// </summary>
    /// <param name="rateLimiter">The rate limiter to apply.</param>
    /// <returns>A configured options instance.</returns>
    public static StepExecutionOptions RateLimited(RateLimiter rateLimiter)
    {
        ArgumentNullException.ThrowIfNull(rateLimiter);
        return new StepExecutionOptions(rateLimiter: rateLimiter);
    }

    /// <summary>
    /// Creates options with a display name.
    /// </summary>
    /// <param name="name">The step display name.</param>
    /// <returns>A configured options instance.</returns>
    public static StepExecutionOptions Named(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return new StepExecutionOptions(name: name);
    }
}