using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Persists checkpoints produced by request-response pipeline runs.
/// </summary>
public interface IPipelineCheckpointStore
{
    /// <summary>
    /// Saves a checkpoint.
    /// </summary>
    /// <param name="checkpoint">The checkpoint to save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the save operation.</returns>
    ValueTask SaveAsync(PipelineCheckpoint checkpoint, CancellationToken cancellationToken = default);

    /// <summary>
    /// Loads all checkpoints for a run.
    /// </summary>
    /// <param name="runId">The pipeline run identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stored checkpoints for the run.</returns>
    ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadAsync(
        string runId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Loads all checkpoints for a pipeline.
    /// </summary>
    /// <param name="pipelineId">The pipeline identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stored checkpoints for the pipeline.</returns>
    ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadByPipelineAsync(
        string pipelineId,
        CancellationToken cancellationToken = default);
}