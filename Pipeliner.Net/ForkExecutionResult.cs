using System;
using System.Collections.Generic;
using System.Linq;

namespace Pipeliner.Net;

/// <summary>
/// Represents the output of a forked branch execution.
/// </summary>
/// <typeparam name="T">The branch output type.</typeparam>
public sealed record ForkExecutionResult<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ForkExecutionResult{T}" /> record.
    /// </summary>
    /// <param name="branchResults">The ordered branch results.</param>
    public ForkExecutionResult(IReadOnlyList<ForkResult<T>> branchResults)
    {
        ArgumentNullException.ThrowIfNull(branchResults);
        BranchResults = branchResults;
    }

    /// <summary>
    /// Gets the ordered branch results.
    /// </summary>
    public IReadOnlyList<ForkResult<T>> BranchResults { get; }

    /// <summary>
    /// Gets all successful branch values in branch order.
    /// </summary>
    public IReadOnlyList<T> SuccessfulResults =>
        BranchResults.Where(result => result.IsSuccess).Select(result => result.Value!).ToArray();

    /// <summary>
    /// Gets all branch failures in branch order.
    /// </summary>
    public IReadOnlyList<Exception> Failures =>
        BranchResults.Where(result => !result.IsSuccess).Select(result => result.Error!).ToArray();
}