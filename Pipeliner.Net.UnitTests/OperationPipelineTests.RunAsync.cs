using Serilog.Events;
using Serilog.Sinks.TestCorrelator;

namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    [Fact]
    public async Task RunAsync_EmbeddedPipeline_Success()
    {
        // Arrange
        var logger = CreateLogger();

        var innerPipeline = Pipeline
            .For<int>(logger)
            .Then<int>(x => x + IncrementByFive)
            .Build();

        var pipelineBuilder = Pipeline
            .For<string>(logger)
            .Then<int>(Convert.ToInt32)
            .ThenAsync<int>(async (value, cancellationToken) =>
                await innerPipeline.RunAsync(value, cancellationToken).ConfigureAwait(false));

        var pipeline = pipelineBuilder.Build();

        // Act
        var result = await pipeline.RunAsync(ValidNumber);
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext();

        // Assert
        Assert.Equal(55, result);
        Assert.Equal(EmbeddedPipelineInfoLogCount, logEvents.Count(x => x.Level == LogEventLevel.Information));
    }

    [Fact]
    public async Task RunAsync_ExceptionOccursNoHandler_ThrowsDivideByZeroException()
    {
        // Arrange
        var logger = CreateLogger();

        var pipelineBuilder = Pipeline
            .For<int>(logger)
            // ReSharper disable once IntDivisionByZero - test for exception.
            .Then<int>(x => x / 0);

        var pipeline = pipelineBuilder.Build();

        // Act + Assert
        await Assert.ThrowsAsync<DivideByZeroException>(() => pipeline.RunAsync(10));
    }

    [Fact]
    public async Task RunAsync_WithImplicitResult_Success()
    {
        // Arrange
        var logger = CreateLogger();
        var pipeline = CreateStringIncrementPipelineBuilder(logger).Build();

        // Act
        var result = await pipeline.RunAsync(ValidNumber);
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext();

        // Assert
        Assert.Equal(55, result);
        Assert.Equal(SingleOperationInfoLogCount, logEvents.Count(x => x.Level == LogEventLevel.Information));
    }

    [Fact]
    public async Task RunAsync_EmbeddedPipeline_WithCustomOptions_Success()
    {
        // Arrange
        var logger = CreateLogger();

        var innerPipeline = Pipeline
            .For<int>(logger)
            .Then<int>(x => x + IncrementByFive)
            .Build();

        var completionValue = 0;

        var pipelineBuilder = Pipeline
            .For<string>(logger)
            .Then<int>(Convert.ToInt32)
            .ThenAsync<int>(async (value, cancellationToken) =>
            {
                var output = await innerPipeline.RunAsync(value, cancellationToken).ConfigureAwait(false);
                completionValue = output;
                return output;
            });

        var pipeline = pipelineBuilder.Build(EmbeddedOperationName);

        // Act
        var result = await pipeline.RunAsync(ValidNumber);
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext().ToList();

        // Assert
        Assert.Equal(55, result);
        Assert.Equal(55, completionValue);
        Assert.Contains(logEvents, logEvent => logEvent.RenderMessage().Contains(EmbeddedOperationName));
    }

    [Fact]
    public async Task RunAsync_WithAsyncOperations_Success()
    {
        // Arrange
        var logger = CreateLogger();

        var pipelineBuilder = Pipeline
            .For<string>(logger)
            .ThenAsync<int>(async (value, cancellationToken) =>
            {
                await Task.Delay(5, cancellationToken);
                return Convert.ToInt32(value);
            })
            .ThenAsync<int>(async (value, cancellationToken) =>
            {
                await Task.Delay(5, cancellationToken);
                return value + IncrementByFive;
            });

        var pipeline = pipelineBuilder.Build();

        // Act
        var result = await pipeline.RunAsync(ValidNumber);

        // Assert
        Assert.Equal(55, result);
    }

    [Fact]
    public async Task RunAsync_TypeThreadedBuilder_WithAsyncStep_Success()
    {
        // Arrange
        var pipelineBuilder = Pipeline
            .For<string>()
            .Then<int>(Convert.ToInt32)
            .Then<int>(async (value, cancellationToken) =>
            {
                await Task.Delay(5, cancellationToken);
                return value + IncrementByFive;
            });

        var pipeline = pipelineBuilder.Build();

        // Act
        var result = await pipeline.RunAsync(ValidNumber);

        // Assert
        Assert.Equal(55, result);
    }
}