using System;
using System.Collections.Generic;
using System.Linq;

namespace Pipeliner.Net;

/// <summary>
/// Represents the result of a pipeline dry-run validation.
/// </summary>
public sealed class PipelineDryRunReport
{
    private PipelineDryRunReport(PipelineDefinition definition, IEnumerable<PipelineDryRunIssue> issues)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(issues);

        Definition = definition;
        Issues = issues.ToArray();
    }

    /// <summary>
    /// Gets the validated pipeline definition.
    /// </summary>
    public PipelineDefinition Definition { get; }

    /// <summary>
    /// Gets the dry-run validation issues.
    /// </summary>
    public IReadOnlyList<PipelineDryRunIssue> Issues { get; }

    /// <summary>
    /// Gets a value indicating whether the pipeline definition is structurally valid.
    /// </summary>
    public bool IsValid => Issues.All(issue => issue.Severity != PipelineDryRunIssueSeverity.Error);

    internal static PipelineDryRunReport Validate(PipelineDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        var issues = new List<PipelineDryRunIssue>();
        var nodeIds = new HashSet<string>(StringComparer.Ordinal);
        var duplicateNodeIds = new HashSet<string>(StringComparer.Ordinal);

        foreach (var node in definition.Nodes)
        {
            if (!nodeIds.Add(node.Id))
                duplicateNodeIds.Add(node.Id);

            if (string.IsNullOrWhiteSpace(node.Name))
                issues.Add(new PipelineDryRunIssue(
                    PipelineDryRunIssueSeverity.Warning,
                    "node.name.empty",
                    "Pipeline node has an empty display name.",
                    node.Id));
        }

        foreach (var duplicateNodeId in duplicateNodeIds)
        {
            issues.Add(new PipelineDryRunIssue(
                PipelineDryRunIssueSeverity.Error,
                "node.id.duplicate",
                $"Pipeline node id `{duplicateNodeId}` is duplicated.",
                duplicateNodeId));
        }

        if (definition.Nodes.Count == 0)
        {
            issues.Add(new PipelineDryRunIssue(
                PipelineDryRunIssueSeverity.Error,
                "graph.nodes.empty",
                "Pipeline definition does not contain any nodes."));
        }

        if (!definition.Nodes.Any(node => node.Kind == PipelineNodeKind.Input))
        {
            issues.Add(new PipelineDryRunIssue(
                PipelineDryRunIssueSeverity.Error,
                "graph.input.missing",
                "Pipeline definition does not contain an input node."));
        }

        foreach (var edge in definition.Edges)
        {
            if (!nodeIds.Contains(edge.From))
            {
                issues.Add(new PipelineDryRunIssue(
                    PipelineDryRunIssueSeverity.Error,
                    "edge.from.missing",
                    $"Pipeline edge source node `{edge.From}` does not exist.",
                    edge.From));
            }

            if (!nodeIds.Contains(edge.To))
            {
                issues.Add(new PipelineDryRunIssue(
                    PipelineDryRunIssueSeverity.Error,
                    "edge.to.missing",
                    $"Pipeline edge destination node `{edge.To}` does not exist.",
                    edge.To));
            }
        }

        AddReachabilityIssues(definition, issues);
        return new PipelineDryRunReport(definition, issues);
    }

    private static void AddReachabilityIssues(PipelineDefinition definition, ICollection<PipelineDryRunIssue> issues)
    {
        var inputNode = definition.Nodes.FirstOrDefault(node => node.Kind == PipelineNodeKind.Input);
        if (inputNode is null)
            return;

        var nodesById = definition.Nodes.ToDictionary(node => node.Id, StringComparer.Ordinal);
        var outgoingEdges = definition.Edges
            .Where(edge => nodesById.ContainsKey(edge.From) && nodesById.ContainsKey(edge.To))
            .GroupBy(edge => edge.From, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Select(edge => edge.To).ToArray(), StringComparer.Ordinal);

        var reachable = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Stack<string>();
        pending.Push(inputNode.Id);

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            if (!reachable.Add(current))
                continue;

            if (!outgoingEdges.TryGetValue(current, out var destinations))
                continue;

            foreach (var destination in destinations)
                pending.Push(destination);
        }

        foreach (var node in definition.Nodes.Where(node => !reachable.Contains(node.Id)))
        {
            issues.Add(new PipelineDryRunIssue(
                PipelineDryRunIssueSeverity.Error,
                "node.unreachable",
                $"Pipeline node `{node.Name}` is not reachable from the input node.",
                node.Id));
        }
    }
}