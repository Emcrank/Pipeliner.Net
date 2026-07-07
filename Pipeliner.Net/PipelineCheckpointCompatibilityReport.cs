using System.Collections.Generic;
using System.Linq;

namespace Pipeliner.Net;

/// <summary>
/// Describes whether a checkpoint is compatible with a pipeline definition.
/// </summary>
public sealed class PipelineCheckpointCompatibilityReport
{
    /// <summary>
    /// Initializes a new instance of <see cref="PipelineCheckpointCompatibilityReport" />.
    /// </summary>
    /// <param name="issues">Compatibility issues.</param>
    public PipelineCheckpointCompatibilityReport(IEnumerable<string> issues)
    {
        Issues = issues.ToArray();
    }

    /// <summary>
    /// Gets a value indicating whether the checkpoint is compatible.
    /// </summary>
    public bool IsCompatible => Issues.Count == 0;

    /// <summary>
    /// Gets compatibility issues.
    /// </summary>
    public IReadOnlyList<string> Issues { get; }
}
