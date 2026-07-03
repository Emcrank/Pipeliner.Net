using System;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Represents a typed operation that can execute synchronously or asynchronously within an
/// <see cref="OperationPipeline{TParam,TResult}" />.
/// </summary>
/// <typeparam name="TInput">The operation input type.</typeparam>
/// <typeparam name="TResult">The operation output type.</typeparam>
public abstract class Operation<TInput, TResult> : IUntypedOperation
{
    /// <summary>
    /// Gets the synchronous execution delegate for this operation.
    /// </summary>
    public abstract Func<TInput, TResult?> Execution { get; }

    /// <summary>
    /// Gets the completion callback invoked after successful execution.
    /// </summary>
    public virtual Action<TResult> OnCompletionHandler => DelegateDefaults.OnCompletionHandler<TResult>.Empty;

    /// <summary>
    /// Gets the operation display name.
    /// </summary>
    public virtual string Name => "Unnamed_Operation";

    /// <summary>
    /// Gets the exception handler callback.
    /// </summary>
    public virtual Func<Exception, bool> OnExceptionHandler => DelegateDefaults.OnExceptionHandler.Empty;

    /// <summary>
    /// Gets a predicate that determines whether this operation should run.
    /// </summary>
    public virtual Func<bool> CanExecute => DelegateDefaults.CanExecute.Always;

    Func<object?, object?> IUntypedOperation.UntypedExecution => value => Execution((TInput)value!);

    Func<object?, CancellationToken, ValueTask<object?>> IUntypedOperation.UntypedExecutionAsync =>
        async (value, cancellationToken) => await ExecuteAsync((TInput)value!, cancellationToken).ConfigureAwait(false);

    Action<object?> IUntypedOperation.UntypedOnCompletionHandler =>
        value => OnCompletionHandler.Invoke((TResult)value!);

    /// <summary>
    /// Executes the operation asynchronously.
    /// </summary>
    /// <param name="input">The operation input.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The operation output.</returns>
    public virtual ValueTask<TResult?> ExecuteAsync(TInput input, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(Execution(input));
}