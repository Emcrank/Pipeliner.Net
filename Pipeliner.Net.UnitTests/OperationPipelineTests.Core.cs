using MicrosoftLogger = Microsoft.Extensions.Logging.ILogger;
using Serilog;
using Serilog.Extensions.Logging;
using Serilog.Sinks.TestCorrelator;

namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests : IDisposable
{
    private const string ValidNumber = "50";
    private const int IncrementByFive = 5;
    private const int SingleOperationInfoLogCount = 4;
    private const int EmbeddedPipelineInfoLogCount = 8;
    private const string EmbeddedOperationName = "Embedded operation";

    private readonly ITestCorrelatorContext testContext;
    private readonly SerilogLoggerFactory loggerFactory;

    public OperationPipelineTests()
    {
        Log.Logger = new LoggerConfiguration()
            .WriteTo.TestCorrelator()
            .Enrich.FromLogContext()
            .MinimumLevel.Debug()
            .CreateLogger();

        loggerFactory = new SerilogLoggerFactory(Log.Logger);
        testContext = TestCorrelator.CreateContext();
    }

    public void Dispose()
    {
        testContext.Dispose();
    }

    private MicrosoftLogger CreateLogger() => loggerFactory.CreateLogger(nameof(OperationPipelineTests));

    private static PipelineBuilder<string, int> CreateStringIncrementPipelineBuilder(MicrosoftLogger? logger = null) =>
        Pipeline
            .For<string>(logger)
            .Then<int>(Convert.ToInt32)
            .Then<int>(value => value + IncrementByFive);

    private static OperationPipeline<string, int> CreateStringIncrementPipeline(MicrosoftLogger? logger = null) =>
        CreateStringIncrementPipelineBuilder(logger).Build();
}