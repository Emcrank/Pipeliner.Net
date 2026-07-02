using System;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

/// <summary>
/// Represents an <see cref="Operation{TInput,TResult}" /> implementation backed by delegates.
/// </summary>
/// <typeparam name="TInput">The operation input type.</typeparam>
/// <typeparam name="TOutput">The operation output type.</typeparam>
public sealed class DelegateOperation<TInput, TOutput> : Operation<TInput, TOutput>
{
    private readonly Func<TInput, CancellationToken, ValueTask<TOutput?>> executionAsync;

    /// <summary>
    /// Initializes a new instance of <see cref="DelegateOperation{TInput,TOutput}" /> using a synchronous execution delegate.
    /// </summary>
    /// <param name="execution">The synchronous execution delegate.</param>
    /// <param name="operationName">The optional operation name.</param>
    /// <param name="canExecute">The optional execution predicate.</param>
    /// <param name="onCompletionHandler">The optional completion callback.</param>
    /// <param name="onExceptionHandler">The optional exception handler callback.</param>
    public DelegateOperation(
        Func<TInput, TOutput?> execution,
        string? operationName = null,
        Func<bool>? canExecute = null,
        Action<TOutput>? onCompletionHandler = null,
        Func<Exception, bool>? onExceptionHandler = null)
    {
        ArgumentNullException.ThrowIfNull(execution);

        Execution = execution;
        executionAsync = (input, _) => ValueTask.FromResult(Execution(input));

        Name = operationName ?? base.Name;
        CanExecute = canExecute ?? base.CanExecute;
        OnCompletionHandler = onCompletionHandler ?? base.OnCompletionHandler;
        OnExceptionHandler = onExceptionHandler ?? base.OnExceptionHandler;
    }

    /// <summary>
    /// Initializes a new instance of <see cref="DelegateOperation{TInput,TOutput}" /> using an asynchronous execution
    /// delegate.
    /// </summary>
    /// <param name="execution">The asynchronous execution delegate.</param>
    /// <param name="operationName">The optional operation name.</param>
    /// <param name="canExecute">The optional execution predicate.</param>
    /// <param name="onCompletionHandler">The optional completion callback.</param>
    /// <param name="onExceptionHandler">The optional exception handler callback.</param>
    public DelegateOperation(
        Func<TInput, CancellationToken, ValueTask<TOutput?>> execution,
        string? operationName = null,
        Func<bool>? canExecute = null,
        Action<TOutput>? onCompletionHandler = null,
        Func<Exception, bool>? onExceptionHandler = null)
    {
        ArgumentNullException.ThrowIfNull(execution);

        executionAsync = async (input, cancellationToken) =>
            await execution(input, cancellationToken).ConfigureAwait(false);
        Execution = input => executionAsync(input, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        Name = operationName ?? base.Name;
        CanExecute = canExecute ?? base.CanExecute;
        OnCompletionHandler = onCompletionHandler ?? base.OnCompletionHandler;
        OnExceptionHandler = onExceptionHandler ?? base.OnExceptionHandler;
    }

    /// <inheritdoc />
    public override Func<TInput, TOutput?> Execution { get; }

    /// <inheritdoc />
    public override string Name { get; }

    /// <inheritdoc />
    public override Func<bool> CanExecute { get; }

    /// <inheritdoc />
    public override Action<TOutput> OnCompletionHandler { get; }

    /// <inheritdoc />
    public override Func<Exception, bool> OnExceptionHandler { get; }

    /// <inheritdoc />
    public override ValueTask<TOutput?> ExecuteAsync(TInput input, CancellationToken cancellationToken = default) =>
        executionAsync(input, cancellationToken);
}