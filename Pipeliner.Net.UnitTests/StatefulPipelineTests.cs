namespace Pipeliner.Net.UnitTests;

public sealed class StatefulPipelineTests
{
    [Fact]
    public async Task RunAsyncWithStateCreatesNewStateForEachRun()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .WithState(() => new CounterState())
            .Then("Increment state", (value, state) =>
            {
                state.Count++;
                return value + state.Count;
            })
            .Then("Increment state again", (value, state) =>
            {
                state.Count++;
                return value + state.Count;
            })
            .Build();

        // Act
        var first = await pipeline.RunAsync(10);
        var second = await pipeline.RunAsync(10);

        // Assert
        Assert.Equal(13, first);
        Assert.Equal(13, second);
    }

    [Fact]
    public async Task RunAsyncWithStateIsolatesConcurrentRuns()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .WithState(() => new CounterState())
            .ThenAsync(
                "Use isolated state",
                async (value, state, cancellationToken) =>
                {
                    state.Count++;
                    await Task.Delay(25, cancellationToken);
                    state.Count++;
                    return value + state.Count;
                })
            .Build();

        // Act
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => pipeline.RunAsync(10)));

        // Assert
        Assert.All(results, result => Assert.Equal(12, result));
    }

    [Fact]
    public void DescribeWhenStatefulStepConfiguredIncludesStepNode()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .WithState(() => new CounterState())
            .Then("Stateful step", (value, state) =>
            {
                state.Count++;
                return value + state.Count;
            })
            .Build();

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Contains(definition.Nodes, node => node.Name == "Stateful step" && node.Kind == PipelineNodeKind.Step);
    }

    private sealed class CounterState
    {
        public int Count { get; set; }
    }
}