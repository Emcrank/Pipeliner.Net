using System.Threading.RateLimiting;

namespace Pipeliner.Net.UnitTests;

public sealed class StepExecutionOptionsTests
{
    [Fact]
    public async Task RunAsyncThenAsyncWithMaxConcurrencyLimitsConcurrentExecutions()
    {
        // Arrange
        var activeExecutions = 0;
        var maxObservedExecutions = 0;

        var pipeline = Pipeline
            .For<int>()
            .ThenAsync(
                "Limited step",
                async (value, cancellationToken) =>
                {
                    var active = Interlocked.Increment(ref activeExecutions);
                    UpdateMax(ref maxObservedExecutions, active);

                    try
                    {
                        await Task.Delay(50, cancellationToken);
                        return value;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref activeExecutions);
                    }
                },
                StepExecutionOptions.WithMaxConcurrency(2))
            .Build();

        // Act
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(value => pipeline.RunAsync(value)));

        // Assert
        Assert.Equal(Enumerable.Range(0, 8), results);
        Assert.Equal(2, maxObservedExecutions);
    }

    [Fact]
    public async Task RunAsyncThenAsyncWithRateLimiterThrowsWhenLeaseIsRejected()
    {
        // Arrange
        using var limiter = new ConcurrencyLimiter(
            new ConcurrencyLimiterOptions
            {
                PermitLimit = 1,
                QueueLimit = 0,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst
            });

        var enteredStep = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseStep = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pipeline = Pipeline
            .For<int>()
            .ThenAsync(
                "Rate limited step",
                async (value, cancellationToken) =>
                {
                    enteredStep.TrySetResult();
                    await releaseStep.Task.WaitAsync(cancellationToken);
                    return value;
                },
                StepExecutionOptions.RateLimited(limiter))
            .Build();

        var firstRun = pipeline.RunAsync(1);
        await enteredStep.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Act
        var exception = await Assert.ThrowsAsync<PipelineRateLimitRejectedException>(() => pipeline.RunAsync(2));

        // Assert
        Assert.Equal("Rate limited step", exception.StepName);

        releaseStep.SetResult();
        Assert.Equal(1, await firstRun);
    }

    [Fact]
    public void DescribeWhenOptionsNameConfiguredUsesOptionsName()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .ThenAsync(
                (value, _) => ValueTask.FromResult(value + 1),
                StepExecutionOptions.Create(name: "Named from options"))
            .Build();

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Contains(definition.Nodes, node => node.Name == "Named from options");
    }

    [Fact]
    public void WithMaxConcurrencyWhenValueIsInvalidThrowsArgumentOutOfRangeException()
    {
        // Act + Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => StepExecutionOptions.WithMaxConcurrency(0));
    }

    private static void UpdateMax(ref int target, int candidate)
    {
        while (true)
        {
            var current = Volatile.Read(ref target);
            if (candidate <= current)
                return;

            if (Interlocked.CompareExchange(ref target, candidate, current) == current)
                return;
        }
    }
}