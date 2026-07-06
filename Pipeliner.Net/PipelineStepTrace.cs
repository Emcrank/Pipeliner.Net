using System;

namespace Pipeliner.Net;

/// <summary>
/// Represents trace metadata for one pipeline step execution.
/// </summary>
/// <param name="Name">The step display name.</param>
/// <param name="Kind">The step kind.</param>
/// <param name="InputType">The step input type.</param>
/// <param name="OutputType">The step output type.</param>
/// <param name="Duration">The step execution duration.</param>
/// <param name="Succeeded">Whether the step completed successfully.</param>
/// <param name="ExceptionType">The exception type when the step failed.</param>
public sealed record PipelineStepTrace(
    string Name,
    PipelineNodeKind Kind,
    Type InputType,
    Type OutputType,
    TimeSpan Duration,
    bool Succeeded,
    string? ExceptionType = null);