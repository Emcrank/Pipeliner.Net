using System;
using System.Collections.Generic;

namespace Pipeliner.Net;

/// <summary>
/// Represents the output of a forked branch execution.
/// </summary>
/// <typeparam name="T">The successful branch output type.</typeparam>
/// <param name="SuccessfulResults">The successful branch results in branch order.</param>
/// <param name="Failures">The branch failures in branch order.</param>
public sealed record ForkExecutionResult<T>(IReadOnlyList<T> SuccessfulResults, IReadOnlyList<Exception> Failures);