using System;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Represents an untyped pipeline operation contract used for heterogeneous execution chains.
/// </summary>
internal interface IUntypedOperation
{
    /// <summary>
    /// Gets the operation display name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the synchronous untyped execution delegate.
    /// </summary>
    Func<object?, object?> UntypedExecution { get; }

    /// <summary>
    /// Gets the asynchronous untyped execution delegate.
    /// </summary>
    Func<object?, CancellationToken, ValueTask<object?>> UntypedExecutionAsync { get; }

    /// <summary>
    /// Gets the completion callback for the untyped result.
    /// </summary>
    Action<object?>? UntypedOnCompletionHandler { get; }

    /// <summary>
    /// Gets the predicate that determines whether the operation can execute.
    /// </summary>
    Func<bool> CanExecute { get; }

    /// <summary>
    /// Gets the exception handler delegate.
    /// </summary>
    Func<Exception, bool>? OnExceptionHandler { get; }
}