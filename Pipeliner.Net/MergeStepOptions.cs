namespace Pipeliner.Net;

/// <summary>
/// Configures merge execution behavior.
/// </summary>
/// <param name="ConflictStrategy">The merge conflict strategy.</param>
public sealed record MergeStepOptions(MergeConflictStrategy ConflictStrategy)
{
    /// <summary>
    /// Creates options that throw if any branch fails.
    /// </summary>
    /// <returns>A configured options instance.</returns>
    public static MergeStepOptions AggregateFailures() => ThrowOnAnyFailure();

    /// <summary>
    /// Creates options that execute a custom reducer.
    /// </summary>
    /// <returns>A configured options instance.</returns>
    public static MergeStepOptions CustomReducer() => new(MergeConflictStrategy.CustomReducer);

    /// <summary>
    /// Creates options that return the first successful branch output.
    /// </summary>
    /// <returns>A configured options instance.</returns>
    public static MergeStepOptions FirstSuccess() => TakeFirst();

    /// <summary>
    /// Creates options that ignore failed branches and continue with successful results.
    /// </summary>
    /// <returns>A configured options instance.</returns>
    public static MergeStepOptions IgnoreFailures() => new(MergeConflictStrategy.IgnoreFailures);

    /// <summary>
    /// Creates options that return the first successful branch output.
    /// </summary>
    /// <returns>A configured options instance.</returns>
    public static MergeStepOptions TakeFirst() => new(MergeConflictStrategy.TakeFirst);

    /// <summary>
    /// Creates options that throw if any branch fails.
    /// </summary>
    /// <returns>A configured options instance.</returns>
    public static MergeStepOptions ThrowOnAnyFailure() => new(MergeConflictStrategy.ThrowOnAnyFailure);
}