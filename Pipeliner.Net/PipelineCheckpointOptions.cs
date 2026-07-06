using System;
using System.Text.Json;

namespace Pipeliner.Net;

/// <summary>
/// Configures checkpoint persistence for request-response pipelines.
/// </summary>
public sealed record PipelineCheckpointOptions
{
    /// <summary>
    /// Initializes a new instance of <see cref="PipelineCheckpointOptions" />.
    /// </summary>
    /// <param name="store">The checkpoint store.</param>
    /// <param name="serializerOptions">Optional JSON serializer options.</param>
    /// <param name="failureBehavior">Checkpoint persistence failure behavior.</param>
    public PipelineCheckpointOptions(
        IPipelineCheckpointStore store,
        JsonSerializerOptions? serializerOptions = null,
        PipelineCheckpointFailureBehavior failureBehavior = PipelineCheckpointFailureBehavior.FailRun)
    {
        ArgumentNullException.ThrowIfNull(store);

        Store = store;
        SerializerOptions = serializerOptions ?? PipelineCheckpointJson.DefaultSerializerOptions;
        FailureBehavior = failureBehavior;
    }

    /// <summary>
    /// Gets the checkpoint store.
    /// </summary>
    public IPipelineCheckpointStore Store { get; init; }

    /// <summary>
    /// Gets the JSON serializer options used for checkpoint payloads.
    /// </summary>
    public JsonSerializerOptions SerializerOptions { get; init; }

    /// <summary>
    /// Gets the persistence failure behavior.
    /// </summary>
    public PipelineCheckpointFailureBehavior FailureBehavior { get; init; }

    /// <summary>
    /// Creates checkpoint options that fail the run when persistence fails.
    /// </summary>
    /// <param name="store">The checkpoint store.</param>
    /// <param name="serializerOptions">Optional JSON serializer options.</param>
    /// <returns>A configured options instance.</returns>
    public static PipelineCheckpointOptions FailRun(
        IPipelineCheckpointStore store,
        JsonSerializerOptions? serializerOptions = null) =>
        new(store, serializerOptions, PipelineCheckpointFailureBehavior.FailRun);

    /// <summary>
    /// Creates checkpoint options that continue the run when persistence fails.
    /// </summary>
    /// <param name="store">The checkpoint store.</param>
    /// <param name="serializerOptions">Optional JSON serializer options.</param>
    /// <returns>A configured options instance.</returns>
    public static PipelineCheckpointOptions ContinueOnFailure(
        IPipelineCheckpointStore store,
        JsonSerializerOptions? serializerOptions = null) =>
        new(store, serializerOptions, PipelineCheckpointFailureBehavior.Continue);
}
