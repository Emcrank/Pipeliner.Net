namespace Pipeliner.Net.UnitTests;

public sealed class SagaPipelineTests
{
    [Fact]
    public async Task RunAsyncWhenDownstreamStepFailsCompensatesCompletedSagaStepsInReverseOrder()
    {
        // Arrange
        var events = new List<string>();

        var pipeline = Pipeline
            .For<int>()
            .ThenSaga(
                "Reserve inventory",
                (value, _) =>
                {
                    events.Add("reserve");
                    return ValueTask.FromResult(value + 1);
                },
                (value, _) =>
                {
                    events.Add($"release:{value}");
                    return ValueTask.CompletedTask;
                })
            .ThenSaga(
                "Capture payment",
                (value, _) =>
                {
                    events.Add("capture");
                    return ValueTask.FromResult(value + 1);
                },
                (value, _) =>
                {
                    events.Add($"refund:{value}");
                    return ValueTask.CompletedTask;
                })
            .ThenAsync<int>((_, _) => ValueTask.FromException<int>(new InvalidOperationException("boom")))
            .Build();

        // Act + Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.RunAsync(10));
        Assert.Equal(["reserve", "capture", "refund:12", "release:11"], events);
    }

    [Fact]
    public async Task RunAsyncWhenPipelineSucceedsDoesNotCompensateSagaSteps()
    {
        // Arrange
        var compensated = false;

        var pipeline = Pipeline
            .For<int>()
            .ThenSaga(
                "Create customer",
                (value, _) => ValueTask.FromResult(value + 1),
                (_, _) =>
                {
                    compensated = true;
                    return ValueTask.CompletedTask;
                })
            .Build();

        // Act
        var result = await pipeline.RunAsync(10);

        // Assert
        Assert.Equal(11, result);
        Assert.False(compensated);
    }

    [Fact]
    public async Task RunAsyncWhenCompensationFailsThrowsPipelineSagaCompensationException()
    {
        // Arrange
        var originalException = new InvalidOperationException("pipeline failed");
        var compensationException = new InvalidOperationException("compensation failed");

        var pipeline = Pipeline
            .For<int>()
            .ThenSaga(
                "Create subscription",
                (value, _) => ValueTask.FromResult(value + 1),
                (_, _) => ValueTask.FromException(compensationException))
            .ThenAsync<int>((_, _) => ValueTask.FromException<int>(originalException))
            .Build();

        // Act
        var exception = await Assert.ThrowsAsync<PipelineSagaCompensationException>(() => pipeline.RunAsync(10));

        // Assert
        Assert.Same(originalException, exception.OriginalException);
        Assert.Single(exception.CompensationFailures);
        Assert.Same(compensationException, exception.CompensationFailures[0]);
    }

    [Fact]
    public void DescribeWhenSagaStepConfiguredIncludesSagaNode()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .ThenSaga(
                "Compensatable step",
                (value, _) => ValueTask.FromResult(value + 1),
                (_, _) => ValueTask.CompletedTask)
            .Build();

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Contains(definition.Nodes, node =>
            node.Name == "Compensatable step" && node.Kind == PipelineNodeKind.Saga);
    }
}