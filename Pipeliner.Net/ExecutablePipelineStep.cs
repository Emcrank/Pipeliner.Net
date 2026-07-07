using System;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

internal sealed class ExecutablePipelineStep<TInput, TOutput> : IExecutablePipelineStep
{
    private readonly Func<TInput, PipelineExecutionContext, CancellationToken, ValueTask<TOutput>> execute;

    public ExecutablePipelineStep(
        string id,
        string name,
        PipelineNodeKind kind,
        Func<TInput, PipelineExecutionContext, CancellationToken, ValueTask<TOutput>> execute)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(execute);

        Id = id;
        Name = name;
        Kind = kind;
        this.execute = execute;
    }

    public string Id { get; }

    public string Name { get; }

    public Type InputType => typeof(TInput);

    public Type OutputType => typeof(TOutput);

    public PipelineNodeKind Kind { get; }

    public async ValueTask<object?> ExecuteAsync(
        object? input,
        PipelineExecutionContext context,
        CancellationToken cancellationToken) =>
        await context.ExecuteStepAsync(
            Id,
            Name,
            Kind,
            async (stepContext, token) => await execute((TInput)input!, stepContext, token).ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);
}