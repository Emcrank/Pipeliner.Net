using System;

namespace Pipeliner.Net;

/// <summary>
/// Represents a single fork branch execution outcome.
/// </summary>
/// <typeparam name="T">The branch output type.</typeparam>
/// <param name="Index">The branch index in registration order.</param>
/// <param name="IsSuccess">A value indicating whether the branch completed successfully.</param>
/// <param name="Value">The branch value when successful.</param>
/// <param name="Error">The branch exception when failed.</param>
public sealed record ForkResult<T>(int Index, bool IsSuccess, T? Value, Exception? Error);