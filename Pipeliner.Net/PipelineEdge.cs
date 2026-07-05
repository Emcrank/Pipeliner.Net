namespace Pipeliner.Net;

/// <summary>
/// Represents a directed edge between two pipeline definition nodes.
/// </summary>
/// <param name="From">The source node identifier.</param>
/// <param name="To">The destination node identifier.</param>
/// <param name="Label">The optional edge label.</param>
public sealed record PipelineEdge(string From, string To, string? Label = null);
