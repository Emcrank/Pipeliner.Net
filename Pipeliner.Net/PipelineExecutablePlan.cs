using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

internal sealed class PipelineExecutablePlan<TInput, TOutput>
{
    private readonly IReadOnlyList<IExecutablePipelineStep> steps;

    private PipelineExecutablePlan(IReadOnlyList<IExecutablePipelineStep> steps)
    {
        this.steps = steps;
    }

    public IReadOnlyList<IExecutablePipelineStep> Steps => steps;

    public static PipelineExecutablePlan<TInput, TInput> Create() => new([]);

    public async ValueTask<TOutput> ExecuteAsync(
        TInput input,
        PipelineExecutionContext context,
        CancellationToken cancellationToken)
    {
        object? current = input;

        foreach (var step in steps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            current = await step.ExecuteAsync(current, context, cancellationToken).ConfigureAwait(false);
        }

        return (TOutput)current!;
    }

    public PipelineExecutablePlan<TInput, TNext> Then<TNext>(
        string id,
        string name,
        PipelineNodeKind kind,
        Func<TOutput, PipelineExecutionContext, CancellationToken, ValueTask<TNext>> execute)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(execute);

        return new PipelineExecutablePlan<TInput, TNext>(
            steps
                .Concat([new ExecutablePipelineStep<TOutput, TNext>(id, name, kind, execute)])
                .ToArray());
    }
}