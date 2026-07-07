using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Observes pipeline run, step, halt, and checkpoint events.
/// </summary>
public interface IPipelineObserver
{
    /// <summary>
    /// Called when a pipeline run starts.
    /// </summary>
    ValueTask OnRunStartedAsync(PipelineRunStarted started, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a pipeline run completes.
    /// </summary>
    ValueTask OnRunCompletedAsync(PipelineRunCompleted completed, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a pipeline run fails.
    /// </summary>
    ValueTask OnRunFailedAsync(PipelineRunFailed failed, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a pipeline run halts.
    /// </summary>
    ValueTask OnRunHaltedAsync(PipelineRunHalted halted, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a pipeline step starts.
    /// </summary>
    ValueTask OnStepStartedAsync(PipelineStepStarted started, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a pipeline step completes.
    /// </summary>
    ValueTask OnStepCompletedAsync(PipelineStepCompleted completed, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a pipeline step fails.
    /// </summary>
    ValueTask OnStepFailedAsync(PipelineStepFailed failed, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a pipeline step halts the run.
    /// </summary>
    ValueTask OnStepHaltedAsync(PipelineStepHalted halted, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;

    /// <summary>
    /// Called when a checkpoint is saved.
    /// </summary>
    ValueTask OnCheckpointSavedAsync(PipelineCheckpointSaved saved, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}
