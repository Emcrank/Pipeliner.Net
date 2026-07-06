namespace Pipeliner.Net;

/// <summary>
/// Determines how checkpoint persistence failures affect pipeline execution.
/// </summary>
public enum PipelineCheckpointFailureBehavior
{
    /// <summary>
    /// Propagates checkpoint persistence failures to the caller.
    /// </summary>
    FailRun,

    /// <summary>
    /// Ignores checkpoint persistence failures and allows the run to continue.
    /// </summary>
    Continue
}
