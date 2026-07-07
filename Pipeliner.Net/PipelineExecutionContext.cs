using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Provides metadata and helpers for the active pipeline execution.
/// </summary>
public sealed class PipelineExecutionContext
{
    private static readonly AsyncLocal<PipelineExecutionContext?> CurrentContext = new();
    private readonly List<PipelineStepAttempt> attempts;
    private readonly IReadOnlyList<IPipelineObserver> observers;

    internal PipelineExecutionContext(
        string runId,
        string pipelineId,
        string pipelineName,
        string pipelineVersion,
        PipelineCheckpointOptions? checkpointOptions,
        IEnumerable<IPipelineObserver>? observers = null,
        List<PipelineStepAttempt>? attempts = null,
        string? stepId = null,
        string? stepName = null,
        PipelineNodeKind? stepKind = null,
        int attemptNumber = 0)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineName);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineVersion);

        RunId = runId;
        PipelineId = pipelineId;
        PipelineName = pipelineName;
        PipelineVersion = pipelineVersion;
        CheckpointOptions = checkpointOptions;
        StepId = stepId;
        StepName = stepName;
        StepKind = stepKind;
        AttemptNumber = attemptNumber;
        this.observers = observers?.ToArray() ?? [];
        this.attempts = attempts ?? [];
    }

    /// <summary>
    /// Gets the current execution context for this async flow, if one exists.
    /// </summary>
    public static PipelineExecutionContext? Current => CurrentContext.Value;

    /// <summary>
    /// Gets the run identifier.
    /// </summary>
    public string RunId { get; }

    /// <summary>
    /// Gets the pipeline identifier.
    /// </summary>
    public string PipelineId { get; }

    /// <summary>
    /// Gets the pipeline display name.
    /// </summary>
    public string PipelineName { get; }

    /// <summary>
    /// Gets the pipeline version.
    /// </summary>
    public string PipelineVersion { get; }

    /// <summary>
    /// Gets the current step identifier when the context is scoped to a step.
    /// </summary>
    public string? StepId { get; }

    /// <summary>
    /// Gets the current step display name when the context is scoped to a step.
    /// </summary>
    public string? StepName { get; }

    /// <summary>
    /// Gets the current step kind when the context is scoped to a step.
    /// </summary>
    public PipelineNodeKind? StepKind { get; }

    /// <summary>
    /// Gets the current attempt number when the context is scoped to a step.
    /// </summary>
    public int AttemptNumber { get; }

    /// <summary>
    /// Gets recorded step attempts for the current run.
    /// </summary>
    public IReadOnlyList<PipelineStepAttempt> Attempts => attempts;

    internal PipelineCheckpointOptions? CheckpointOptions { get; }

    internal static async ValueTask<T> RunAsync<T>(
        PipelineExecutionContext context,
        Func<CancellationToken, ValueTask<T>> execution,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(execution);

        var previous = CurrentContext.Value;
        CurrentContext.Value = context;

        try
        {
            return await execution(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            CurrentContext.Value = previous;
        }
    }

    internal PipelineExecutionContext ForStep(
        string stepId,
        string stepName,
        PipelineNodeKind stepKind,
        int attemptNumber) =>
        new(
            RunId,
            PipelineId,
            PipelineName,
            PipelineVersion,
            CheckpointOptions,
            observers,
            attempts,
            stepId,
            stepName,
            stepKind,
            attemptNumber);

    internal async ValueTask<T> ExecuteStepAsync<T>(
        string stepId,
        string stepName,
        PipelineNodeKind stepKind,
        Func<PipelineExecutionContext, CancellationToken, ValueTask<T>> execution,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stepId);
        ArgumentException.ThrowIfNullOrWhiteSpace(stepName);
        ArgumentNullException.ThrowIfNull(execution);

        var attemptNumber = GetNextAttemptNumber(stepId);
        var stepContext = ForStep(stepId, stepName, stepKind, attemptNumber);
        var startedAt = DateTimeOffset.UtcNow;
        long started = Stopwatch.GetTimestamp();

        await NotifyStepStartedAsync(stepContext, startedAt, cancellationToken).ConfigureAwait(false);

        try
        {
            var result = await execution(stepContext, cancellationToken).ConfigureAwait(false);
            var attempt = AddAttempt(stepContext, PipelineStepAttemptStatus.Completed, startedAt, started);
            await NotifyStepCompletedAsync(attempt, cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch (PipelineHaltedException)
        {
            var attempt = AddAttempt(stepContext, PipelineStepAttemptStatus.Halted, startedAt, started);
            await NotifyStepHaltedAsync(attempt, cancellationToken).ConfigureAwait(false);
            throw;
        }
        catch (Exception exception)
        {
            var attempt = AddAttempt(
                stepContext,
                PipelineStepAttemptStatus.Failed,
                startedAt,
                started,
                exception.GetType().FullName,
                exception.Message);
            await NotifyStepFailedAsync(attempt, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    internal async ValueTask NotifyRunStartedAsync(DateTimeOffset startedAt, CancellationToken cancellationToken)
    {
        var started = new PipelineRunStarted(RunId, PipelineId, PipelineName, PipelineVersion, startedAt);
        foreach (var observer in observers)
            await observer.OnRunStartedAsync(started, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask NotifyRunCompletedAsync(
        DateTimeOffset startedAt,
        DateTimeOffset completedAt,
        TimeSpan duration,
        CancellationToken cancellationToken)
    {
        var completed = new PipelineRunCompleted(
            RunId,
            PipelineId,
            PipelineName,
            PipelineVersion,
            startedAt,
            completedAt,
            duration);

        foreach (var observer in observers)
            await observer.OnRunCompletedAsync(completed, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask NotifyRunFailedAsync(
        DateTimeOffset startedAt,
        DateTimeOffset failedAt,
        TimeSpan duration,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var failed = new PipelineRunFailed(
            RunId,
            PipelineId,
            PipelineName,
            PipelineVersion,
            startedAt,
            failedAt,
            duration,
            exception.GetType().FullName ?? exception.GetType().Name,
            exception.Message);

        foreach (var observer in observers)
            await observer.OnRunFailedAsync(failed, cancellationToken).ConfigureAwait(false);
    }

    internal async ValueTask NotifyRunHaltedAsync(PipelineHaltedException exception, CancellationToken cancellationToken)
    {
        var halted = new PipelineRunHalted(
            RunId,
            PipelineId,
            PipelineName,
            PipelineVersion,
            exception.HaltName,
            exception.NodeId,
            DateTimeOffset.UtcNow);

        foreach (var observer in observers)
            await observer.OnRunHaltedAsync(halted, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Saves a checkpoint for the current run.
    /// </summary>
    /// <typeparam name="TPayload">The payload type.</typeparam>
    /// <param name="checkpointName">The checkpoint name.</param>
    /// <param name="nodeId">The checkpoint node identifier.</param>
    /// <param name="payload">The checkpoint payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the checkpoint save operation.</returns>
    public async ValueTask SaveCheckpointAsync<TPayload>(
        string checkpointName,
        string nodeId,
        TPayload payload,
        CancellationToken cancellationToken = default)
    {
        if (CheckpointOptions is null)
            return;

        try
        {
            var payloadJson = JsonSerializer.Serialize(payload, CheckpointOptions.SerializerOptions);
            var payloadType = typeof(TPayload).AssemblyQualifiedName ?? typeof(TPayload).FullName ?? typeof(TPayload).Name;
            var checkpoint = new PipelineCheckpoint(
                RunId,
                PipelineId,
                PipelineName,
                checkpointName,
                nodeId,
                payloadType,
                payloadJson,
                DateTimeOffset.UtcNow,
                PipelineVersion);

            await CheckpointOptions.Store.SaveAsync(checkpoint, cancellationToken).ConfigureAwait(false);
            foreach (var observer in observers)
                await observer.OnCheckpointSavedAsync(new PipelineCheckpointSaved(checkpoint), cancellationToken)
                    .ConfigureAwait(false);
        }
        catch when (CheckpointOptions.FailureBehavior == PipelineCheckpointFailureBehavior.Continue)
        {
        }
    }

    private int GetNextAttemptNumber(string stepId)
    {
        lock (attempts)
        {
            return attempts.Count(attempt => attempt.StepId == stepId) + 1;
        }
    }

    private PipelineStepAttempt AddAttempt(
        PipelineExecutionContext stepContext,
        PipelineStepAttemptStatus status,
        DateTimeOffset startedAt,
        long started,
        string? exceptionType = null,
        string? exceptionMessage = null)
    {
        var completedAt = DateTimeOffset.UtcNow;
        var attempt = new PipelineStepAttempt(
            RunId,
            PipelineId,
            PipelineName,
            PipelineVersion,
            stepContext.StepId!,
            stepContext.StepName!,
            stepContext.StepKind!.Value,
            stepContext.AttemptNumber,
            status,
            startedAt,
            completedAt,
            Stopwatch.GetElapsedTime(started),
            exceptionType,
            exceptionMessage);

        lock (attempts)
        {
            attempts.Add(attempt);
        }

        return attempt;
    }

    private async ValueTask NotifyStepStartedAsync(
        PipelineExecutionContext stepContext,
        DateTimeOffset startedAt,
        CancellationToken cancellationToken)
    {
        var started = new PipelineStepStarted(
            RunId,
            PipelineId,
            PipelineName,
            PipelineVersion,
            stepContext.StepId!,
            stepContext.StepName!,
            stepContext.StepKind!.Value,
            stepContext.AttemptNumber,
            startedAt);

        foreach (var observer in observers)
            await observer.OnStepStartedAsync(started, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask NotifyStepCompletedAsync(PipelineStepAttempt attempt, CancellationToken cancellationToken)
    {
        var completed = new PipelineStepCompleted(attempt);
        foreach (var observer in observers)
            await observer.OnStepCompletedAsync(completed, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask NotifyStepFailedAsync(PipelineStepAttempt attempt, CancellationToken cancellationToken)
    {
        var failed = new PipelineStepFailed(attempt);
        foreach (var observer in observers)
            await observer.OnStepFailedAsync(failed, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask NotifyStepHaltedAsync(PipelineStepAttempt attempt, CancellationToken cancellationToken)
    {
        var halted = new PipelineStepHalted(attempt);
        foreach (var observer in observers)
            await observer.OnStepHaltedAsync(halted, cancellationToken).ConfigureAwait(false);
    }
}