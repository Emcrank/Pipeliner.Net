using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

internal sealed class PipelineTraceContext
{
    private static readonly AsyncLocal<PipelineTraceContext?> CurrentContext = new();
    private readonly List<PipelineStepTrace> steps = [];

    public static PipelineTraceContext? Current => CurrentContext.Value;

    public static PipelineRunResult<T> Run<T>(Func<T> execution)
    {
        ArgumentNullException.ThrowIfNull(execution);

        var previousContext = CurrentContext.Value;
        var context = new PipelineTraceContext();
        CurrentContext.Value = context;

        try
        {
            var result = execution();
            return new PipelineRunResult<T>(result, context.ToTrace());
        }
        finally
        {
            CurrentContext.Value = previousContext;
        }
    }

    public static async Task<PipelineRunResult<T>> RunAsync<T>(Func<Task<T>> execution)
    {
        ArgumentNullException.ThrowIfNull(execution);

        var previousContext = CurrentContext.Value;
        var context = new PipelineTraceContext();
        CurrentContext.Value = context;

        try
        {
            var result = await execution().ConfigureAwait(false);
            return new PipelineRunResult<T>(result, context.ToTrace());
        }
        finally
        {
            CurrentContext.Value = previousContext;
        }
    }

    public async ValueTask<T> TraceStepAsync<T>(
        string stepName,
        PipelineNodeKind kind,
        Type inputType,
        Type outputType,
        Func<ValueTask<T>> execution)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stepName);
        ArgumentNullException.ThrowIfNull(inputType);
        ArgumentNullException.ThrowIfNull(outputType);
        ArgumentNullException.ThrowIfNull(execution);

        long startTimestamp = Stopwatch.GetTimestamp();

        try
        {
            var result = await execution().ConfigureAwait(false);
            steps.Add(new PipelineStepTrace(
                stepName,
                kind,
                inputType,
                outputType,
                Stopwatch.GetElapsedTime(startTimestamp),
                true));
            return result;
        }
        catch (Exception exception)
        {
            steps.Add(new PipelineStepTrace(
                stepName,
                kind,
                inputType,
                outputType,
                Stopwatch.GetElapsedTime(startTimestamp),
                false,
                exception.GetType().FullName));
            throw;
        }
    }

    private PipelineRunTrace ToTrace() => new(steps);
}