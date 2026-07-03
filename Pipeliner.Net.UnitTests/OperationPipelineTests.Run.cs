using Serilog.Events;
using Serilog.Sinks.TestCorrelator;

namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    [Fact]
    public void Run_EmbeddedPipeline_Success()
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
            .ThenAsync<int>((value, cancellationToken) =>
                new ValueTask<int>(innerPipeline.RunAsync(value, cancellationToken)));

        var pipeline = pipelineBuilder.Build();

        // Act
        var result = pipeline.Run(ValidNumber);
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext();

        // Assert
        Assert.Equal(55, result);
        Assert.Equal(EmbeddedPipelineInfoLogCount, logEvents.Count(x => x.Level == LogEventLevel.Information));
    }

    [Fact]
    public void Run_ExceptionOccursNoHandler_ThrowsDivideByZeroException()
    {
        // Arrange
        var logger = CreateLogger();

        var pipelineBuilder = Pipeline
            .For<int>(logger)
            // ReSharper disable once IntDivisionByZero - test for exception.
            .Then<int>(x => x / 0);

        var pipeline = pipelineBuilder.Build();

        // Act + Assert
        Assert.Throws<DivideByZeroException>(() => pipeline.Run(10));
    }

    [Fact]
    public void Run_WithImplicitResult_Success()
    {
        // Arrange
        var logger = CreateLogger();
        var pipeline = CreateStringIncrementPipelineBuilder(logger).Build();

        // Act
        var result = pipeline.Run(ValidNumber);
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext();

        // Assert
        Assert.Equal(55, result);
        Assert.Equal(SingleOperationInfoLogCount, logEvents.Count(x => x.Level == LogEventLevel.Information));
    }

    [Fact]
    public void Run_EmbeddedPipeline_WithCustomOptions_Success()
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
        var result = pipeline.Run(ValidNumber);
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext().ToList();

        // Assert
        Assert.Equal(55, result);
        Assert.Equal(55, completionValue);
        Assert.Contains(logEvents, logEvent => logEvent.RenderMessage().Contains(EmbeddedOperationName));
    }

    [Fact]
    public void Run_TypeThreadedBuilder_Success()
    {
        // Arrange
        var pipeline = CreateStringIncrementPipelineBuilder().Build();

        // Act
        var result = pipeline.Run(ValidNumber);

        // Assert
        Assert.Equal(55, result);
    }
}