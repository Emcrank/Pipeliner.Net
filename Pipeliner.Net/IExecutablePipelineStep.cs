using System;
using System.Threading;
using System.Threading.Tasks;

namespace Pipeliner.Net;

internal interface IExecutablePipelineStep
{
    string Id { get; }

    string Name { get; }

    Type InputType { get; }

    Type OutputType { get; }

    PipelineNodeKind Kind { get; }

    ValueTask<object?> ExecuteAsync(object? input, PipelineExecutionContext context, CancellationToken cancellationToken);
}
