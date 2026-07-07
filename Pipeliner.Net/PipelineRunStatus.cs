namespace Pipeliner.Net;

/// <summary>
/// Describes the final status of a pipeline run.
/// </summary>
public enum PipelineRunStatus
{
    /// <summary>
    /// The pipeline completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// The pipeline halted at a controlled halt point.
    /// </summary>
    Halted,

    /// <summary>
    /// The pipeline failed with an exception.
    /// </summary>
    Failed,

    /// <summary>
    /// The pipeline was cancelled.
    /// </summary>
    Cancelled
}