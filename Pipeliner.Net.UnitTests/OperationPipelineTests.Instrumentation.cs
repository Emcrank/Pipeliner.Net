using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    [Fact]
    public void Run_Instrumentation_EmitsActivities()
    {
        // Arrange
        int startedCount = 0;
        int stoppedCount = 0;

        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Pipeliner.Net",
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = _ => startedCount++,
            ActivityStopped = _ => stoppedCount++
        };

        ActivitySource.AddActivityListener(activityListener);

        var pipeline = new OperationPipeline<string, int>()
            .AddOperation<string, int>(Convert.ToInt32)
            .AddOperation<int, int>(value => value + 5);

        // Act
        _ = pipeline.Run("50");

        // Assert
        Assert.True(startedCount > 0);
        Assert.True(stoppedCount > 0);
    }

    [Fact]
    public void Run_Instrumentation_EmitsMeterMetrics()
    {
        // Arrange
        long runCount = 0;

        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Pipeliner.Net")
                listener.EnableMeasurementEvents(instrument);
        };

        meterListener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
        {
            if (instrument.Name == "pipeliner.pipeline.runs")
                runCount += measurement;
        });

        meterListener.Start();

        var pipeline = new OperationPipeline<string, int>()
            .AddOperation<string, int>(Convert.ToInt32)
            .AddOperation<int, int>(value => value + 5);

        // Act
        _ = pipeline.Run("50");

        // Assert
        Assert.True(runCount >= 1);
    }
}
