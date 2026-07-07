using System;

namespace Pipeliner.Net;

/// <summary>
/// Indicates that a pipeline halted at a controlled halt point.
/// </summary>
public sealed class PipelineHaltedException : Exception
{
    /// <summary>
    /// Initializes a new instance of <see cref="PipelineHaltedException" />.
    /// </summary>
    /// <param name="runId">The run identifier.</param>
    /// <param name="haltName">The halt point name.</param>
    /// <param name="nodeId">The halt node identifier.</param>
    public PipelineHaltedException(string runId, string haltName, string nodeId)
        : base($"Pipeline run `{runId}` halted at `{haltName}`.")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        ArgumentException.ThrowIfNullOrWhiteSpace(haltName);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

        RunId = runId;
        HaltName = haltName;
        NodeId = nodeId;
    }

    /// <summary>
    /// Gets the run identifier.
    /// </summary>
    public string RunId { get; }

    /// <summary>
    /// Gets the halt point name.
    /// </summary>
    public string HaltName { get; }

    /// <summary>
    /// Gets the halt node identifier.
    /// </summary>
    public string NodeId { get; }
}
