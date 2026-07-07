namespace Pipeliner.Net;

/// <summary>
/// Describes the outcome of a recorded step attempt.
/// </summary>
public enum PipelineStepAttemptStatus
{
    /// <summary>
    /// The step attempt completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// The step attempt halted the pipeline.
    /// </summary>
    Halted,

    /// <summary>
    /// The step attempt failed.
    /// </summary>
    Failed
}
