using System;
using System.Collections.Generic;

namespace Pipeliner.Net;

internal sealed class PipelineGraph
{
    private PipelineGraph(
        IReadOnlyList<PipelineNode> nodes,
        IReadOnlyList<PipelineEdge> edges,
        string terminalNodeId,
        int nextNodeIndex)
    {
        Nodes = nodes;
        Edges = edges;
        TerminalNodeId = terminalNodeId;
        NextNodeIndex = nextNodeIndex;
    }

    private IReadOnlyList<PipelineNode> Nodes { get; }

    private IReadOnlyList<PipelineEdge> Edges { get; }

    private string TerminalNodeId { get; }

    private int NextNodeIndex { get; }

    public PipelineNode TerminalNode
    {
        get
        {
            foreach (var node in Nodes)
            {
                if (node.Id == TerminalNodeId)
                    return node;
            }

            throw new InvalidOperationException("Pipeline graph terminal node was not found.");
        }
    }

    public static PipelineGraph Create(Type inputType)
    {
        var inputNode = new PipelineNode("input", "Input", inputType, inputType, PipelineNodeKind.Input);
        return new PipelineGraph([inputNode], [], inputNode.Id, 1);
    }

    public PipelineGraph AddBranch(string? name, Type inputType, Type outputType)
    {
        var branchName = CreateDefaultName(name, PipelineNodeKind.Branch, NextNodeIndex);
        var branchNode = CreateNode(branchName, inputType, outputType, PipelineNodeKind.Branch, NextNodeIndex);
        var trueNode = CreateNode($"{branchName}: true", inputType, outputType, PipelineNodeKind.BranchPath, NextNodeIndex + 1);
        var falseNode = CreateNode($"{branchName}: false", inputType, outputType, PipelineNodeKind.BranchPath, NextNodeIndex + 2);
        var joinNode = CreateNode($"{branchName}: join", outputType, outputType, PipelineNodeKind.Merge, NextNodeIndex + 3);

        var nodes = new List<PipelineNode>(Nodes)
        {
            branchNode,
            trueNode,
            falseNode,
            joinNode
        };

        var edges = new List<PipelineEdge>(Edges)
        {
            new(TerminalNodeId, branchNode.Id),
            new(branchNode.Id, trueNode.Id, "true"),
            new(branchNode.Id, falseNode.Id, "false"),
            new(trueNode.Id, joinNode.Id),
            new(falseNode.Id, joinNode.Id)
        };

        return new PipelineGraph(nodes, edges, joinNode.Id, NextNodeIndex + 4);
    }

    public PipelineGraph AddFork(string? name, Type inputType, Type branchOutputType, Type outputType, int branchCount)
    {
        var forkName = CreateDefaultName(name, PipelineNodeKind.Fork, NextNodeIndex);
        var forkNode = CreateNode(forkName, inputType, outputType, PipelineNodeKind.Fork, NextNodeIndex);
        var joinNode = CreateNode($"{forkName}: result", outputType, outputType, PipelineNodeKind.ForkJoin, NextNodeIndex + branchCount + 1);

        var nodes = new List<PipelineNode>(Nodes) { forkNode };
        var edges = new List<PipelineEdge>(Edges)
        {
            new(TerminalNodeId, forkNode.Id)
        };

        for (var index = 0; index < branchCount; index++)
        {
            var branchNode = CreateNode(
                $"{forkName}: branch {index}",
                inputType,
                branchOutputType,
                PipelineNodeKind.BranchPath,
                NextNodeIndex + index + 1);

            nodes.Add(branchNode);
            edges.Add(new PipelineEdge(forkNode.Id, branchNode.Id, $"branch {index}"));
            edges.Add(new PipelineEdge(branchNode.Id, joinNode.Id));
        }

        nodes.Add(joinNode);
        return new PipelineGraph(nodes, edges, joinNode.Id, NextNodeIndex + branchCount + 2);
    }

    public PipelineGraph AddStep(string? name, Type inputType, Type outputType, PipelineNodeKind kind)
    {
        var node = CreateNode(CreateDefaultName(name, kind, NextNodeIndex), inputType, outputType, kind, NextNodeIndex);

        var nodes = new List<PipelineNode>(Nodes) { node };
        var edges = new List<PipelineEdge>(Edges)
        {
            new(TerminalNodeId, node.Id)
        };

        return new PipelineGraph(nodes, edges, node.Id, NextNodeIndex + 1);
    }

    public PipelineDefinition ToDefinition(string pipelineId, string pipelineName, string? pipelineVersion = null) =>
        new(pipelineId, pipelineName, Nodes, Edges, pipelineVersion);

    private static PipelineNode CreateNode(
        string name,
        Type inputType,
        Type outputType,
        PipelineNodeKind kind,
        int index) =>
        new($"{kind.ToString().ToLowerInvariant()}_{index}", name, inputType, outputType, kind);

    private static string CreateDefaultName(string? configuredName, PipelineNodeKind kind, int index) =>
        string.IsNullOrWhiteSpace(configuredName) ? $"{kind} {index}" : configuredName;
}
