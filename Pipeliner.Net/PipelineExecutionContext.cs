using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

internal sealed class PipelineExecutionContext
{
    private static readonly AsyncLocal<PipelineExecutionContext?> CurrentContext = new();

    public PipelineExecutionContext(
        string runId,
        string pipelineId,
        string pipelineName,
        PipelineCheckpointOptions? checkpointOptions)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineName);

        RunId = runId;
        PipelineId = pipelineId;
        PipelineName = pipelineName;
        CheckpointOptions = checkpointOptions;
    }

    public static PipelineExecutionContext? Current => CurrentContext.Value;

    public string RunId { get; }

    public string PipelineId { get; }

    public string PipelineName { get; }

    public PipelineCheckpointOptions? CheckpointOptions { get; }

    public static async ValueTask<T> RunAsync<T>(
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

    public async ValueTask SaveCheckpointAsync<TPayload>(
        string checkpointName,
        string nodeId,
        TPayload payload,
        CancellationToken cancellationToken)
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
                DateTimeOffset.UtcNow);

            await CheckpointOptions.Store.SaveAsync(checkpoint, cancellationToken).ConfigureAwait(false);
        }
        catch when (CheckpointOptions.FailureBehavior == PipelineCheckpointFailureBehavior.Continue)
        {
        }
    }
}