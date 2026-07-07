using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Pipeliner.Net;

/// <summary>
/// Describes the structure of a built pipeline.
/// </summary>
public sealed class PipelineDefinition
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PipelineDefinition" /> class.
    /// </summary>
    /// <param name="id">The pipeline identifier.</param>
    /// <param name="name">The pipeline display name.</param>
    /// <param name="nodes">The pipeline graph nodes.</param>
    /// <param name="edges">The directed graph edges.</param>
    /// <param name="version">The pipeline definition version.</param>
    public PipelineDefinition(
        string id,
        string name,
        IEnumerable<PipelineNode> nodes,
        IEnumerable<PipelineEdge> edges,
        string? version = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentNullException.ThrowIfNull(edges);

        Id = id;
        Name = name;
        Version = string.IsNullOrWhiteSpace(version) ? "1.0.0" : version;
        Nodes = nodes.ToArray();
        Edges = edges.ToArray();
    }

    /// <summary>
    /// Gets the pipeline identifier.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Gets the pipeline display name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the pipeline definition version.
    /// </summary>
    public string Version { get; }

    /// <summary>
    /// Gets the pipeline graph nodes.
    /// </summary>
    public IReadOnlyList<PipelineNode> Nodes { get; }

    /// <summary>
    /// Gets the directed graph edges.
    /// </summary>
    public IReadOnlyList<PipelineEdge> Edges { get; }

    /// <summary>
    /// Exports the pipeline definition as Mermaid flowchart markup.
    /// </summary>
    /// <returns>A Mermaid flowchart.</returns>
    public string ToMermaid()
    {
        var builder = new StringBuilder();
        builder.AppendLine("flowchart TD");

        foreach (var node in Nodes)
        {
            builder
                .Append("    ")
                .Append(ToMermaidId(node.Id))
                .Append("[\"")
                .Append(EscapeMermaidLabel($"{node.Name}<br/>{node.Kind}<br/>{FormatType(node.InputType)} -> {FormatType(node.OutputType)}"))
                .AppendLine("\"]");
        }

        foreach (var edge in Edges)
        {
            builder
                .Append("    ")
                .Append(ToMermaidId(edge.From))
                .Append(" -->");

            if (!string.IsNullOrWhiteSpace(edge.Label))
            {
                builder
                    .Append("|")
                    .Append(EscapeMermaidLabel(edge.Label))
                    .Append("|");
            }

            builder
                .Append(' ')
                .AppendLine(ToMermaidId(edge.To));
        }

        return builder.ToString();
    }

    /// <summary>
    /// Exports the pipeline definition as Graphviz DOT markup.
    /// </summary>
    /// <returns>A DOT graph.</returns>
    public string ToDot()
    {
        var builder = new StringBuilder();
        builder
            .Append("digraph \"")
            .Append(EscapeDot(Name))
            .AppendLine("\" {");

        foreach (var node in Nodes)
        {
            builder
                .Append("    \"")
                .Append(EscapeDot(node.Id))
                .Append("\" [label=\"")
                .Append(EscapeDot(node.Name))
                .Append("\\n")
                .Append(EscapeDot(node.Kind.ToString()))
                .Append("\\n")
                .Append(EscapeDot($"{FormatType(node.InputType)} -> {FormatType(node.OutputType)}"))
                .AppendLine("\"];");
        }

        foreach (var edge in Edges)
        {
            builder
                .Append("    \"")
                .Append(EscapeDot(edge.From))
                .Append("\" -> \"")
                .Append(EscapeDot(edge.To))
                .Append('"');

            if (!string.IsNullOrWhiteSpace(edge.Label))
            {
                builder
                    .Append(" [label=\"")
                    .Append(EscapeDot(edge.Label))
                    .Append("\"]");
            }

            builder.AppendLine(";");
        }

        builder.AppendLine("}");
        return builder.ToString();
    }

    /// <summary>
    /// Exports the pipeline definition as JSON.
    /// </summary>
    /// <param name="options">Optional serializer options.</param>
    /// <returns>A JSON representation of the pipeline graph.</returns>
    public string ToJson(JsonSerializerOptions? options = null)
    {
        var model = new
        {
            id = Id,
            name = Name,
            version = Version,
            nodes = Nodes.Select(node => new
            {
                id = node.Id,
                name = node.Name,
                kind = node.Kind.ToString(),
                inputType = FormatType(node.InputType),
                outputType = FormatType(node.OutputType)
            }),
            edges = Edges.Select(edge => new
            {
                from = edge.From,
                to = edge.To,
                label = edge.Label
            })
        };

        return JsonSerializer.Serialize(
            model,
            options ?? new JsonSerializerOptions { WriteIndented = true });
    }

    /// <summary>
    /// Validates whether a checkpoint is compatible with this pipeline definition.
    /// </summary>
    /// <param name="checkpoint">The checkpoint to validate.</param>
    /// <returns>A compatibility report.</returns>
    public PipelineCheckpointCompatibilityReport ValidateCheckpointCompatibility(PipelineCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        var issues = new List<string>();

        if (checkpoint.PipelineId != Id)
            issues.Add($"Checkpoint pipeline id `{checkpoint.PipelineId}` does not match definition id `{Id}`.");

        if (!string.Equals(checkpoint.PipelineVersion, Version, StringComparison.Ordinal))
            issues.Add($"Checkpoint version `{checkpoint.PipelineVersion}` does not match definition version `{Version}`.");

        var node = Nodes.FirstOrDefault(candidate => candidate.Id == checkpoint.NodeId);
        if (node is null)
        {
            issues.Add($"Checkpoint node `{checkpoint.NodeId}` does not exist in this definition.");
        }
        else if (!IsTypeCompatible(node.OutputType, checkpoint.PayloadType))
        {
            issues.Add($"Checkpoint payload type `{checkpoint.PayloadType}` is not compatible with node `{node.Id}` output type `{node.OutputType.FullName}`.");
        }

        return new PipelineCheckpointCompatibilityReport(issues);
    }

    private static bool IsTypeCompatible(Type expectedType, string payloadType) =>
        payloadType == expectedType.AssemblyQualifiedName ||
        payloadType == expectedType.FullName ||
        payloadType == expectedType.Name;

    private static string EscapeDot(string value) =>
        value
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\"", "\\\"", StringComparison.Ordinal);

    private static string EscapeMermaidLabel(string value) =>
        value
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\"", "&quot;", StringComparison.Ordinal)
            .Replace("|", "&#124;", StringComparison.Ordinal);

    private static string FormatType(Type type)
    {
        if (!type.IsGenericType)
            return type.Name;

        var genericTypeName = type.Name;
        var tickIndex = genericTypeName.IndexOf('`', StringComparison.Ordinal);
        if (tickIndex >= 0)
            genericTypeName = genericTypeName[..tickIndex];

        return $"{genericTypeName}<{string.Join(", ", type.GetGenericArguments().Select(FormatType))}>";
    }

    private static string ToMermaidId(string id)
    {
        var builder = new StringBuilder(id.Length);

        foreach (var character in id)
            builder.Append(char.IsLetterOrDigit(character) || character == '_' ? character : '_');

        return builder.ToString();
    }
}