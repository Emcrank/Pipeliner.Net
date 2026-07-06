namespace Pipeliner.Net;

/// <summary>
/// Represents a dry-run validation issue.
/// </summary>
/// <param name="Severity">The issue severity.</param>
/// <param name="Code">The stable issue code.</param>
/// <param name="Message">The issue message.</param>
/// <param name="NodeId">The optional related node identifier.</param>
public sealed record PipelineDryRunIssue(
    PipelineDryRunIssueSeverity Severity,
    string Code,
    string Message,
    string? NodeId = null);