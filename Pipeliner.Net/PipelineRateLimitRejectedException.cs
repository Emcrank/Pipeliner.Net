using System;

namespace Pipeliner.Net;

/// <summary>
/// The exception thrown when a configured step rate limiter rejects execution.
/// </summary>
public sealed class PipelineRateLimitRejectedException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PipelineRateLimitRejectedException" /> class.
    /// </summary>
    /// <param name="stepName">The rejected step name.</param>
    public PipelineRateLimitRejectedException(string stepName)
        : base($"Rate limiter rejected execution for pipeline step `{stepName}`.")
    {
        StepName = stepName;
    }

    /// <summary>
    /// Gets the rejected step name.
    /// </summary>
    public string StepName { get; }
}