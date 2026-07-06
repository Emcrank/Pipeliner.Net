namespace Pipeliner.Net;

/// <summary>
/// Represents a pipeline result with execution trace metadata.
/// </summary>
/// <typeparam name="TResult">The pipeline result type.</typeparam>
/// <param name="Result">The pipeline result.</param>
/// <param name="Trace">The captured trace.</param>
public sealed record PipelineRunResult<TResult>(TResult? Result, PipelineRunTrace Trace);