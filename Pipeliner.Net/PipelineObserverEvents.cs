using System;

namespace Pipeliner.Net;

/// <summary>
/// Event raised when a pipeline run starts.
/// </summary>
public sealed record PipelineRunStarted(
    string RunId,
    string PipelineId,
    string PipelineName,
    string PipelineVersion,
    DateTimeOffset StartedAt);

/// <summary>
/// Event raised when a pipeline run completes.
/// </summary>
public sealed record PipelineRunCompleted(
    string RunId,
    string PipelineId,
    string PipelineName,
    string PipelineVersion,
    DateTimeOffset StartedAt,
    DateTimeOffset CompletedAt,
    TimeSpan Duration);

/// <summary>
/// Event raised when a pipeline run fails.
/// </summary>
public sealed record PipelineRunFailed(
    string RunId,
    string PipelineId,
    string PipelineName,
    string PipelineVersion,
    DateTimeOffset StartedAt,
    DateTimeOffset FailedAt,
    TimeSpan Duration,
    string ExceptionType,
    string ExceptionMessage);

/// <summary>
/// Event raised when a pipeline run halts.
/// </summary>
public sealed record PipelineRunHalted(
    string RunId,
    string PipelineId,
    string PipelineName,
    string PipelineVersion,
    string HaltName,
    string NodeId,
    DateTimeOffset HaltedAt);

/// <summary>
/// Event raised when a pipeline step starts.
/// </summary>
public sealed record PipelineStepStarted(
    string RunId,
    string PipelineId,
    string PipelineName,
    string PipelineVersion,
    string StepId,
    string StepName,
    PipelineNodeKind StepKind,
    int AttemptNumber,
    DateTimeOffset StartedAt);

/// <summary>
/// Event raised when a pipeline step completes.
/// </summary>
public sealed record PipelineStepCompleted(PipelineStepAttempt Attempt);

/// <summary>
/// Event raised when a pipeline step fails.
/// </summary>
public sealed record PipelineStepFailed(PipelineStepAttempt Attempt);

/// <summary>
/// Event raised when a pipeline step halts the run.
/// </summary>
public sealed record PipelineStepHalted(PipelineStepAttempt Attempt);

/// <summary>
/// Event raised when a checkpoint is saved.
/// </summary>
public sealed record PipelineCheckpointSaved(PipelineCheckpoint Checkpoint);
