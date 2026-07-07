using System;

namespace Pipeliner.Net;

/// <summary>
/// Describes a controlled halt produced by a pipeline run.
/// </summary>
/// <param name="RunId">The run identifier.</param>
/// <param name="PipelineId">The pipeline identifier.</param>
/// <param name="PipelineName">The pipeline display name.</param>
/// <param name="PipelineVersion">The pipeline definition version.</param>
/// <param name="HaltName">The halt point name.</param>
/// <param name="NodeId">The halt node identifier.</param>
public sealed record PipelineHalt(
    string RunId,
    string PipelineId,
    string PipelineName,
    string PipelineVersion,
    string HaltName,
    string NodeId);