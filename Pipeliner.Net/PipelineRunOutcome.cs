using System;

namespace Pipeliner.Net;

/// <summary>
/// Represents the non-throwing outcome of a pipeline run.
/// </summary>
/// <typeparam name="TResult">The pipeline result type.</typeparam>
public sealed class PipelineRunOutcome<TResult>
{
    private PipelineRunOutcome(
        PipelineRunStatus status,
        TResult? value = default,
        PipelineHalt? halt = null,
        Exception? exception = null)
    {
        Status = status;
        Value = value;
        Halt = halt;
        Exception = exception;
    }

    /// <summary>
    /// Gets the final run status.
    /// </summary>
    public PipelineRunStatus Status { get; }

    /// <summary>
    /// Gets a value indicating whether the run completed successfully.
    /// </summary>
    public bool IsCompleted => Status == PipelineRunStatus.Completed;

    /// <summary>
    /// Gets a value indicating whether the run halted at a controlled halt point.
    /// </summary>
    public bool IsHalted => Status == PipelineRunStatus.Halted;

    /// <summary>
    /// Gets a value indicating whether the run failed with an exception.
    /// </summary>
    public bool IsFailed => Status == PipelineRunStatus.Failed;

    /// <summary>
    /// Gets a value indicating whether the run was cancelled.
    /// </summary>
    public bool IsCancelled => Status == PipelineRunStatus.Cancelled;

    /// <summary>
    /// Gets the completed pipeline value when <see cref="IsCompleted" /> is true.
    /// </summary>
    public TResult? Value { get; }

    /// <summary>
    /// Gets halt metadata when <see cref="IsHalted" /> is true.
    /// </summary>
    public PipelineHalt? Halt { get; }

    /// <summary>
    /// Gets the captured exception when <see cref="IsFailed" /> or <see cref="IsCancelled" /> is true.
    /// </summary>
    public Exception? Exception { get; }

    /// <summary>
    /// Creates a completed run outcome.
    /// </summary>
    /// <param name="value">The pipeline result value.</param>
    /// <returns>A completed run outcome.</returns>
    public static PipelineRunOutcome<TResult> Completed(TResult? value) => new(PipelineRunStatus.Completed, value);

    /// <summary>
    /// Creates a halted run outcome.
    /// </summary>
    /// <param name="halt">The halt metadata.</param>
    /// <returns>A halted run outcome.</returns>
    public static PipelineRunOutcome<TResult> Halted(PipelineHalt halt)
    {
        ArgumentNullException.ThrowIfNull(halt);
        return new PipelineRunOutcome<TResult>(PipelineRunStatus.Halted, halt: halt);
    }

    /// <summary>
    /// Creates a failed run outcome.
    /// </summary>
    /// <param name="exception">The failure exception.</param>
    /// <returns>A failed run outcome.</returns>
    public static PipelineRunOutcome<TResult> Failed(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new PipelineRunOutcome<TResult>(PipelineRunStatus.Failed, exception: exception);
    }

    /// <summary>
    /// Creates a cancelled run outcome.
    /// </summary>
    /// <param name="exception">The cancellation exception.</param>
    /// <returns>A cancelled run outcome.</returns>
    public static PipelineRunOutcome<TResult> Cancelled(OperationCanceledException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new PipelineRunOutcome<TResult>(PipelineRunStatus.Cancelled, exception: exception);
    }
}