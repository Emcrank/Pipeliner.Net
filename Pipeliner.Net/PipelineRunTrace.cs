using System;
using System.Collections.Generic;
using System.Linq;

namespace Pipeliner.Net;

/// <summary>
/// Represents trace metadata captured during a pipeline run.
/// </summary>
public sealed class PipelineRunTrace
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PipelineRunTrace" /> class.
    /// </summary>
    /// <param name="steps">The captured step traces.</param>
    public PipelineRunTrace(IEnumerable<PipelineStepTrace> steps)
    {
        ArgumentNullException.ThrowIfNull(steps);
        Steps = steps.ToArray();
    }

    /// <summary>
    /// Gets the captured step traces.
    /// </summary>
    public IReadOnlyList<PipelineStepTrace> Steps { get; }

    /// <summary>
    /// Gets the total captured step duration.
    /// </summary>
    public TimeSpan TotalStepDuration => TimeSpan.FromTicks(Steps.Sum(step => step.Duration.Ticks));
}