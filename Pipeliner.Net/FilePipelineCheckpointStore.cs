using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Stores pipeline checkpoints as JSON files on disk.
/// </summary>
public sealed class FilePipelineCheckpointStore : IPipelineCheckpointStore
{
    private readonly string directoryPath;
    private readonly JsonSerializerOptions serializerOptions;

    /// <summary>
    /// Initializes a new instance of <see cref="FilePipelineCheckpointStore" />.
    /// </summary>
    /// <param name="directoryPath">The directory where checkpoint files are stored.</param>
    /// <param name="serializerOptions">Optional JSON serializer options for checkpoint records.</param>
    public FilePipelineCheckpointStore(string directoryPath, JsonSerializerOptions? serializerOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);

        this.directoryPath = directoryPath;
        this.serializerOptions = serializerOptions ?? PipelineCheckpointJson.DefaultSerializerOptions;
    }

    /// <inheritdoc />
    public async ValueTask SaveAsync(PipelineCheckpoint checkpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        cancellationToken.ThrowIfCancellationRequested();

        Directory.CreateDirectory(directoryPath);

        var fileName = string.Join(
            "_",
            Sanitize(checkpoint.RunId),
            DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmssfffffff"),
            Sanitize(checkpoint.NodeId),
            Guid.NewGuid().ToString("N")[..8]) + ".json";
        var filePath = Path.Combine(directoryPath, fileName);

        await using var stream = File.Create(filePath);
        await JsonSerializer.SerializeAsync(stream, checkpoint, serializerOptions, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadAsync(
        string runId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!Directory.Exists(directoryPath))
            return [];

        var prefix = $"{Sanitize(runId)}_";
        var checkpoints = new List<PipelineCheckpoint>();

        foreach (var filePath in Directory.EnumerateFiles(directoryPath, $"{prefix}*.json").OrderBy(path => path))
        {
            var checkpoint = await ReadCheckpointAsync(filePath, cancellationToken).ConfigureAwait(false);
            if (checkpoint is not null)
                checkpoints.Add(checkpoint);
        }

        return checkpoints;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadByPipelineAsync(
        string pipelineId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!Directory.Exists(directoryPath))
            return [];

        var checkpoints = new List<PipelineCheckpoint>();

        foreach (var filePath in Directory.EnumerateFiles(directoryPath, "*.json").OrderBy(path => path))
        {
            var checkpoint = await ReadCheckpointAsync(filePath, cancellationToken).ConfigureAwait(false);
            if (checkpoint is not null && checkpoint.PipelineId == pipelineId)
                checkpoints.Add(checkpoint);
        }

        return checkpoints;
    }

    private async ValueTask<PipelineCheckpoint?> ReadCheckpointAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(filePath);
        return await JsonSerializer.DeserializeAsync<PipelineCheckpoint>(
                stream,
                serializerOptions,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static string Sanitize(string value)
    {
        var invalidCharacters = Path.GetInvalidFileNameChars();
        var characters = value.ToCharArray();

        for (var index = 0; index < characters.Length; index++)
        {
            if (Array.IndexOf(invalidCharacters, characters[index]) >= 0)
                characters[index] = '_';
        }

        return new string(characters);
    }
}