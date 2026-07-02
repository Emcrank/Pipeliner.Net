using System;

namespace Pipeliner.Net;

/// <summary>
/// Configures per-step execution behavior.
/// </summary>
/// <param name="Policy">Optional policy applied to the step execution.</param>
public sealed record StepExecutionOptions(IPipelineExecutionPolicy? Policy)
{
    /// <summary>
    /// Creates options with no execution policy.
    /// </summary>
    /// <returns>A default options instance.</returns>
    public static StepExecutionOptions None() => new((IPipelineExecutionPolicy?)null);

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
}