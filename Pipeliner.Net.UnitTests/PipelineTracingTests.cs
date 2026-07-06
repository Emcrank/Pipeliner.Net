namespace Pipeliner.Net.UnitTests;

public sealed class PipelineTracingTests
{
    [Fact]
    public void RunWithTraceCapturesSynchronousStepMetadata()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Then("Add one", value => value + 1)
            .Build();

        // Act
        var result = pipeline.RunWithTrace(10);

        // Assert
        Assert.Equal(11, result.Result);
        var step = Assert.Single(result.Trace.Steps);
        Assert.Equal("Add one", step.Name);
        Assert.Equal(PipelineNodeKind.Step, step.Kind);
        Assert.Equal(typeof(int), step.InputType);
        Assert.Equal(typeof(int), step.OutputType);
        Assert.True(step.Succeeded);
    }

    [Fact]
    public async Task RunWithTraceAsyncCapturesAsynchronousStepMetadata()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Then("Add one", value => value + 1)
            .ThenAsync("Delay add", async (value, cancellationToken) =>
            {
                await Task.Delay(10, cancellationToken);
                return value + 1;
            })
            .Build();

        // Act
        var result = await pipeline.RunWithTraceAsync(10);

        // Assert
        Assert.Equal(12, result.Result);
        Assert.Equal(["Add one", "Delay add"], result.Trace.Steps.Select(step => step.Name));
        Assert.All(result.Trace.Steps, step => Assert.True(step.Succeeded));
        Assert.True(result.Trace.TotalStepDuration > TimeSpan.Zero);
    }

    [Fact]
    public async Task RunWithTraceAsyncCapturesStatefulStepMetadata()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .WithState(() => new CounterState())
            .Then("Use state", (value, state) =>
            {
                state.Count++;
                return value + state.Count;
            })
            .Build();

        // Act
        var result = await pipeline.RunWithTraceAsync(10);

        // Assert
        Assert.Equal(11, result.Result);
        var step = Assert.Single(result.Trace.Steps);
        Assert.Equal("Use state", step.Name);
    }

    private sealed class CounterState
    {
        public int Count { get; set; }
    }
}