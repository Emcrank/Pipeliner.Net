using System;

namespace Pipeliner.Net;

/// <summary>
/// Represents a persisted pipeline checkpoint.
/// </summary>
public sealed record PipelineCheckpoint
{
    /// <summary>
    /// Initializes a new instance of <see cref="PipelineCheckpoint" />.
    /// </summary>
    /// <param name="runId">The pipeline run identifier.</param>
    /// <param name="pipelineId">The pipeline identifier.</param>
    /// <param name="pipelineName">The pipeline display name.</param>
    /// <param name="checkpointName">The checkpoint display name.</param>
    /// <param name="nodeId">The checkpoint node identifier.</param>
    /// <param name="payloadType">The assembly-qualified checkpoint payload type.</param>
    /// <param name="payloadJson">The serialized checkpoint payload.</param>
    /// <param name="createdAt">The checkpoint creation timestamp.</param>
    public PipelineCheckpoint(
        string runId,
        string pipelineId,
        string pipelineName,
        string checkpointName,
        string nodeId,
        string payloadType,
        string payloadJson,
        DateTimeOffset createdAt)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineName);
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointName);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentException.ThrowIfNullOrWhiteSpace(payloadType);
        ArgumentNullException.ThrowIfNull(payloadJson);

        RunId = runId;
        PipelineId = pipelineId;
        PipelineName = pipelineName;
        CheckpointName = checkpointName;
        NodeId = nodeId;
        PayloadType = payloadType;
        PayloadJson = payloadJson;
        CreatedAt = createdAt;
    }

    /// <summary>
    /// Gets the pipeline run identifier.
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
    /// Gets the checkpoint display name.
    /// </summary>
    public string CheckpointName { get; }

    /// <summary>
    /// Gets the checkpoint node identifier.
    /// </summary>
    public string NodeId { get; }

    /// <summary>
    /// Gets the assembly-qualified checkpoint payload type.
    /// </summary>
    public string PayloadType { get; }

    /// <summary>
    /// Gets the serialized checkpoint payload.
    /// </summary>
    public string PayloadJson { get; }

    /// <summary>
    /// Gets the checkpoint creation timestamp.
    /// </summary>
    public DateTimeOffset CreatedAt { get; }
}
