using System;

namespace Pipeliner.Net;

/// <summary>
/// Represents a node in a pipeline definition graph.
/// </summary>
/// <param name="Id">The stable node identifier within the pipeline definition.</param>
/// <param name="Name">The node display name.</param>
/// <param name="InputType">The node input type.</param>
/// <param name="OutputType">The node output type.</param>
/// <param name="Kind">The node role.</param>
public sealed record PipelineNode(
    string Id,
    string Name,
    Type InputType,
    Type OutputType,
    PipelineNodeKind Kind);
