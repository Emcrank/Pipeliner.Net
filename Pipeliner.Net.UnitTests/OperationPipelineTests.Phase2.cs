using System.Linq;

namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    private const string TransientErrorMessage = "Transient error";

    [Fact]
    public async Task RunAsyncBranchRoutesTruePath()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Branch(value => value > 10, value => value + 1, value => value - 1)
            .Build();

        // Act
        var result = await pipeline.RunAsync(20);

        // Assert
        Assert.Equal(21, result);
    }

    [Fact]
    public async Task RunAsyncBranchRoutesFalsePath()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Branch(value => value > 10, value => value + 1, value => value - 1)
            .Build();

        // Act
        var result = await pipeline.RunAsync(3);

        // Assert
        Assert.Equal(2, result);
    }

    [Fact]
    public async Task RunAsyncThenAsyncWithStepPolicyRetriesStep()
    {
        // Arrange
        var executionCount = 0;

        var pipeline = Pipeline
            .For<int>()
            .ThenAsync(
                (value, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    executionCount++;

                    if (executionCount < 2)
                        throw new InvalidOperationException(TransientErrorMessage);

                    return ValueTask.FromResult(value + 1);
                },
                StepExecutionOptions.WithPolicy(new RetryExecutionPolicy(2)))
            .Build();

        // Act
        var result = await pipeline.RunAsync(10);

        // Assert
        Assert.Equal(11, result);
        Assert.Equal(2, executionCount);
    }

    [Fact]
    public async Task RunAsyncForkMergeCustomReducerHandlesPartialFailures()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Fork<int>(
                static (value, _) => ValueTask.FromResult(value + 1),
                static (_, _) => ValueTask.FromException<int>(new InvalidOperationException(TransientErrorMessage)),
                static (value, _) => ValueTask.FromResult(value + 3))
            .Merge<int, int>((results, _) => ValueTask.FromResult(results.Sum()), MergeStepOptions.CustomReducer())
            .Build();

        // Act
        var result = await pipeline.RunAsync(10);

        // Assert
        Assert.Equal(24, result);
    }

    [Fact]
    public async Task RunAsyncForkMergeAggregateFailuresThrows()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Fork<int>(
                static (value, _) => ValueTask.FromResult(value + 1),
                static (_, _) => ValueTask.FromException<int>(new InvalidOperationException(TransientErrorMessage)),
                static (value, _) => ValueTask.FromResult(value + 3))
            .Merge<int, int>((results, _) => ValueTask.FromResult(results.Sum()), MergeStepOptions.AggregateFailures())
            .Build();

        // Act
        var exception = await Assert.ThrowsAsync<AggregateException>(() => pipeline.RunAsync(10));

        // Assert
        Assert.Single(exception.InnerExceptions);
        Assert.IsType<InvalidOperationException>(exception.InnerExceptions[0]);
    }

    [Fact]
    public async Task RunAsyncForkMergeFirstSuccessReturnsFirstResult()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Fork<int>(
                static (value, _) => ValueTask.FromResult(value + 1),
                static (_, _) => ValueTask.FromException<int>(new InvalidOperationException(TransientErrorMessage)),
                static (value, _) => ValueTask.FromResult(value + 3))
            .Merge<int, int>((results, _) => ValueTask.FromResult(results.Sum()), MergeStepOptions.FirstSuccess())
            .Build();

        // Act
        var result = await pipeline.RunAsync(10);

        // Assert
        Assert.Equal(11, result);
    }
}
