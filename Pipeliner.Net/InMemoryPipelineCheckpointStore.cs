using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Stores pipeline checkpoints in memory.
/// </summary>
public sealed class InMemoryPipelineCheckpointStore : IPipelineCheckpointStore
{
    private readonly ConcurrentDictionary<string, List<PipelineCheckpoint>> checkpoints = new();

    /// <inheritdoc />
    public ValueTask SaveAsync(PipelineCheckpoint checkpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        cancellationToken.ThrowIfCancellationRequested();

        var runCheckpoints = checkpoints.GetOrAdd(checkpoint.RunId, _ => []);
        lock (runCheckpoints)
        {
            runCheckpoints.Add(checkpoint);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadAsync(
        string runId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        cancellationToken.ThrowIfCancellationRequested();

        if (!checkpoints.TryGetValue(runId, out var runCheckpoints))
            return ValueTask.FromResult<IReadOnlyList<PipelineCheckpoint>>([]);

        lock (runCheckpoints)
        {
            return ValueTask.FromResult<IReadOnlyList<PipelineCheckpoint>>(runCheckpoints.ToArray());
        }
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadByPipelineAsync(
        string pipelineId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineId);
        cancellationToken.ThrowIfCancellationRequested();

        var matches = new List<PipelineCheckpoint>();

        foreach (var runCheckpoints in checkpoints.Values)
        {
            lock (runCheckpoints)
            {
                matches.AddRange(runCheckpoints.Where(checkpoint => checkpoint.PipelineId == pipelineId));
            }
        }

        return ValueTask.FromResult<IReadOnlyList<PipelineCheckpoint>>(matches
            .OrderBy(checkpoint => checkpoint.CreatedAt)
            .ToArray());
    }
}