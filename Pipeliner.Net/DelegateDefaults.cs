using System;

namespace Pipeliner.Net;

/// <summary>
/// Provides default delegates used by operation implementations.
/// </summary>
internal static class DelegateDefaults
{
    /// <summary>
    /// Provides default can-execute delegates.
    /// </summary>
    internal static class CanExecute
    {
        /// <summary>
        /// Gets a delegate that always returns <see langword="true" />.
        /// </summary>
        internal static Func<bool> Always { get; } = () => true;
    }

    /// <summary>
    /// Provides default completion delegates.
    /// </summary>
    /// <typeparam name="T">The completion value type.</typeparam>
    internal static class OnCompletionHandler<T>
    {
        /// <summary>
        /// Gets a delegate that performs no action.
        /// </summary>
        internal static Action<T> Empty { get; } = _ => { };
    }

    /// <summary>
    /// Provides default exception handler delegates.
    /// </summary>
    internal static class OnExceptionHandler
    {
        /// <summary>
        /// Gets a delegate that indicates exceptions are not handled.
        /// </summary>
        internal static Func<Exception, bool> Empty { get; } = _ => false;
    }
}