using System;
using System.Collections.Generic;
using System.Linq;

namespace Pipeliner.Net;

/// <summary>
/// The exception thrown when one or more saga compensation callbacks fail.
/// </summary>
public sealed class PipelineSagaCompensationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PipelineSagaCompensationException" /> class.
    /// </summary>
    /// <param name="originalException">The original pipeline execution exception.</param>
    /// <param name="compensationFailures">The compensation failures.</param>
    public PipelineSagaCompensationException(
        Exception originalException,
        IReadOnlyList<Exception> compensationFailures)
        : base("One or more saga compensation callbacks failed.", originalException)
    {
        ArgumentNullException.ThrowIfNull(originalException);
        ArgumentNullException.ThrowIfNull(compensationFailures);

        OriginalException = originalException;
        CompensationFailures = compensationFailures.ToArray();
    }

    /// <summary>
    /// Gets the original pipeline execution exception.
    /// </summary>
    public Exception OriginalException { get; }

    /// <summary>
    /// Gets the compensation failures.
    /// </summary>
    public IReadOnlyList<Exception> CompensationFailures { get; }
}