namespace Pipeliner.Net;

/// <summary>
/// Defines merge behavior for forked branch outputs.
/// </summary>
public enum MergeConflictStrategy
{
    /// <summary>
    /// Returns the first successful branch result.
    /// </summary>
    FirstSuccess,

    /// <summary>
    /// Throws an aggregate exception if any branch fails before merge execution.
    /// </summary>
    AggregateFailures,

    /// <summary>
    /// Invokes the custom reducer and allows handling partial successes.
    /// </summary>
    CustomReducer
}