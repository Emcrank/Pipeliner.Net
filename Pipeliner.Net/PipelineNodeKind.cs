namespace Pipeliner.Net;

/// <summary>
/// Describes the role of a node in a pipeline definition graph.
/// </summary>
public enum PipelineNodeKind
{
    /// <summary>
    /// The synthetic pipeline input node.
    /// </summary>
    Input,

    /// <summary>
    /// A standard transformation step.
    /// </summary>
    Step,

    /// <summary>
    /// A conditional branch decision.
    /// </summary>
    Branch,

    /// <summary>
    /// One of the possible branch paths.
    /// </summary>
    BranchPath,

    /// <summary>
    /// A parallel fork decision.
    /// </summary>
    Fork,

    /// <summary>
    /// A synthetic join node for forked branch results.
    /// </summary>
    ForkJoin,

    /// <summary>
    /// A fork result merge step.
    /// </summary>
    Merge,

    /// <summary>
    /// A parallel projection step.
    /// </summary>
    Parallel,

    /// <summary>
    /// An execution policy wrapper.
    /// </summary>
    Policy
}
