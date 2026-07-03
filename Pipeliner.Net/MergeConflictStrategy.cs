namespace Pipeliner.Net;

/// <summary>
/// Defines built-in merge behavior for forked branch outputs.
/// </summary>
public enum MergeConflictStrategy
{
    /// <summary>
    /// Throws an aggregate exception if any branch failed before merge execution.
    /// </summary>
    ThrowOnAnyFailure,

    /// <summary>
    /// Ignores failed branches and continues with successful results only.
    /// </summary>
    IgnoreFailures,

    /// <summary>
    /// Returns the first successful branch result.
    /// </summary>
    TakeFirst,

    /// <summary>
    /// Invokes the custom reducer delegate.
    /// </summary>
    CustomReducer,

    /// <summary>
    /// Backward-compatible alias for <see cref="TakeFirst" />.
    /// </summary>
    FirstSuccess = TakeFirst,

    /// <summary>
    /// Backward-compatible alias for <see cref="ThrowOnAnyFailure" />.
    /// </summary>
    AggregateFailures = ThrowOnAnyFailure
}