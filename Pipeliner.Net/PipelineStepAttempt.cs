using System;

namespace Pipeliner.Net;

/// <summary>
/// Records one execution attempt for a pipeline step.
/// </summary>
public sealed record PipelineStepAttempt(
    string RunId,
    string PipelineId,
    string PipelineName,
    string PipelineVersion,
    string StepId,
    string StepName,
    PipelineNodeKind StepKind,
    int AttemptNumber,
    PipelineStepAttemptStatus Status,
    DateTimeOffset StartedAt,
    DateTimeOffset CompletedAt,
    TimeSpan Duration,
    string? ExceptionType = null,
    string? ExceptionMessage = null);
