namespace Pipeliner.Net.UnitTests;

public sealed class PipelineDryRunTests
{
    [Fact]
    public void DryRunWhenOperationPipelineIsValidReturnsValidReportWithoutExecutingSteps()
    {
        // Arrange
        var executed = false;
        var pipeline = Pipeline
            .For<int>()
            .Then(value =>
            {
                executed = true;
                return value + 1;
            })
            .Build("Dry run pipeline");

        // Act
        var report = pipeline.DryRun();

        // Assert
        Assert.True(report.IsValid);
        Assert.Empty(report.Issues);
        Assert.False(executed);
        Assert.Equal("Dry run pipeline", report.Definition.Name);
    }

    [Fact]
    public void DryRunWhenStreamPipelineIsValidReturnsValidReport()
    {
        // Arrange
        var pipeline = Pipeline
            .StreamFor<int>()
            .Batch(2)
            .Then(batch => batch.Sum())
            .Build("Dry run stream");

        // Act
        var report = pipeline.DryRun();

        // Assert
        Assert.True(report.IsValid);
        Assert.Empty(report.Issues);
        Assert.Equal("Dry run stream", report.Definition.Name);
    }

    [Fact]
    public void ValidateWhenEdgeReferencesMissingNodeReturnsInvalidReport()
    {
        // Arrange
        var definition = new PipelineDefinition(
            "pipeline-id",
            "Invalid pipeline",
            [new PipelineNode("input", "Input", typeof(int), typeof(int), PipelineNodeKind.Input)],
            [new PipelineEdge("input", "missing")]);

        // Act
        var report = PipelineDryRunReport.Validate(definition);

        // Assert
        Assert.False(report.IsValid);
        Assert.Contains(report.Issues, issue => issue.Code == "edge.to.missing");
    }

    [Fact]
    public void ValidateWhenNodeIsUnreachableReturnsInvalidReport()
    {
        // Arrange
        var definition = new PipelineDefinition(
            "pipeline-id",
            "Invalid pipeline",
            [
                new PipelineNode("input", "Input", typeof(int), typeof(int), PipelineNodeKind.Input),
                new PipelineNode("orphan", "Orphan", typeof(int), typeof(int), PipelineNodeKind.Step)
            ],
            []);

        // Act
        var report = PipelineDryRunReport.Validate(definition);

        // Assert
        Assert.False(report.IsValid);
        Assert.Contains(report.Issues, issue => issue.Code == "node.unreachable" && issue.NodeId == "orphan");
    }
}