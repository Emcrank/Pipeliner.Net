using Serilog.Events;
using Serilog.Sinks.TestCorrelator;

namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    [Fact]
    public void Run_EmbeddedPipeline_Success()
    {
        var logger = loggerFactory.CreateLogger(nameof(OperationPipelineTests));

        var innerPipeline = Pipeline
            .For<int>(logger)
            .Then<int>(x => x + 5)
            .Build();

        var pipeline = Pipeline
            .For<string>(logger)
            .Then<int>(Convert.ToInt32)
            .ThenAsync<int>(async (value, cancellationToken) =>
                await innerPipeline.RunAsync(value, cancellationToken).ConfigureAwait(false))
            .Build();

        int result = pipeline.Run("50");
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext();

        Assert.Equal(55, result);
        Assert.Equal(8, logEvents.Count(x => x.Level == LogEventLevel.Information));
    }

    [Fact]
    public void Run_ExceptionOccursAndHandlerHandlesIt_Success()
    {
        var logger = loggerFactory.CreateLogger(nameof(OperationPipelineTests));

        var pipeline = new OperationPipeline<int, int>(logger)
            // ReSharper disable once IntDivisionByZero - test
            .AddOperation<int, int>(x => x / 0, onExceptionHandler: exception => exception is DivideByZeroException);

        int result = pipeline.Run(10);

        Assert.Equal(0, result);
    }

    [Fact]
    public void Run_ExceptionOccursNoHandler_Success()
    {
        var logger = loggerFactory.CreateLogger(nameof(OperationPipelineTests));

        var pipeline = new OperationPipeline<int, int>(logger)
            // ReSharper disable once IntDivisionByZero - test for exception.
            .AddOperation<int, int>(x => x / 0);

        Assert.Throws<DivideByZeroException>(() => pipeline.Run(10));
    }

    [Fact]
    public void Run_WithExplicitResult_Success()
    {
        var logger = loggerFactory.CreateLogger(nameof(OperationPipelineTests));

        int final = 0;

        var pipeline = new OperationPipeline<string, int>(logger)
            .AddOperation<string, int>(Convert.ToInt32)
            .AddOperation<int, int>(
                param =>
                {
                    final = param + 5;
                    return param;
                })
            .SetResult(() => final);

        double result = pipeline.Run("50");
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext();

        Assert.Equal(55, result);
        Assert.Equal(6, logEvents.Count(x => x.Level == LogEventLevel.Information));
    }

    [Fact]
    public void Run_WithImplicitResult_Success()
    {
        var logger = loggerFactory.CreateLogger(nameof(OperationPipelineTests));

        var pipeline = new OperationPipeline<string, int>(logger)
            .AddOperation<string, int>(Convert.ToInt32)
            .AddOperation<int, int>(param => param + 5);

        int result = pipeline.Run("50");
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext();

        Assert.Equal(55, result);
        Assert.Equal(6, logEvents.Count(x => x.Level == LogEventLevel.Information));
    }

    [Fact]
    public void Run_EmbeddedPipeline_WithCustomOptions_Success()
    {
        // Arrange
        const string embeddedOperationName = "Embedded operation";
        var logger = loggerFactory.CreateLogger(nameof(OperationPipelineTests));

        var innerPipeline = Pipeline
            .For<int>(logger)
            .Then<int>(x => x + 5)
            .Build();

        int completionValue = 0;

        var pipeline = Pipeline
            .For<string>(logger)
            .Then<int>(Convert.ToInt32)
            .ThenAsync<int>(async (value, cancellationToken) =>
            {
                var output = await innerPipeline.RunAsync(value, cancellationToken).ConfigureAwait(false);
                completionValue = output;
                return output;
            })
            .Build(embeddedOperationName);

        // Act
        int result = pipeline.Run("50");
        var logEvents = TestCorrelator.GetLogEventsFromCurrentContext().ToList();

        // Assert
        Assert.Equal(55, result);
        Assert.Equal(55, completionValue);
        Assert.Contains(logEvents, logEvent => logEvent.RenderMessage().Contains(embeddedOperationName));
    }

    [Fact]
    public void Run_TypeThreadedBuilder_Success()
    {
        // Arrange
        var pipeline = Pipeline
            .For<string>()
            .Then<int>(Convert.ToInt32)
            .Then<int>(value => value + 5)
            .Build();

        // Act
        int result = pipeline.Run("50");

        // Assert
        Assert.Equal(55, result);
    }
}