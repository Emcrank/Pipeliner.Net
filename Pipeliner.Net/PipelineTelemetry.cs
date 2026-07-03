using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Pipeliner.Net;

/// <summary>
/// Provides shared telemetry instruments for all pipeline instances.
/// </summary>
internal static class PipelineTelemetry
{
    /// <summary>
    /// Gets the activity source used for pipeline tracing.
    /// </summary>
    internal static readonly ActivitySource ActivitySource = new("Pipeliner.Net");

    /// <summary>
    /// Gets the meter used for pipeline metrics.
    /// </summary>
    internal static readonly Meter Meter = new("Pipeliner.Net");

    /// <summary>
    /// Gets the counter tracking total pipeline runs.
    /// </summary>
    internal static readonly Counter<long> PipelineRunCounter = Meter.CreateCounter<long>("pipeliner.pipeline.runs");

    /// <summary>
    /// Gets the counter tracking failed pipeline runs.
    /// </summary>
    internal static readonly Counter<long> PipelineFailureCounter =
        Meter.CreateCounter<long>("pipeliner.pipeline.failures");

    /// <summary>
    /// Gets the histogram tracking total pipeline duration in milliseconds.
    /// </summary>
    internal static readonly Histogram<double> PipelineDurationMs =
        Meter.CreateHistogram<double>("pipeliner.pipeline.duration.ms");

    /// <summary>
    /// Gets the histogram tracking pipeline operation duration in milliseconds.
    /// </summary>
    internal static readonly Histogram<double> PipelineOperationDurationMs =
        Meter.CreateHistogram<double>("pipeliner.pipeline.operation.duration.ms");
}