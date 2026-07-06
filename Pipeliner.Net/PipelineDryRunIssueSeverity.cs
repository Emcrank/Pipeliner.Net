namespace Pipeliner.Net;

/// <summary>
/// Describes the severity of a dry-run validation issue.
/// </summary>
public enum PipelineDryRunIssueSeverity
{
    /// <summary>
    /// The issue is informational or non-blocking.
    /// </summary>
    Warning,

    /// <summary>
    /// The issue indicates an invalid pipeline definition.
    /// </summary>
    Error
}