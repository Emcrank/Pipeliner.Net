namespace Pipeliner.Net.UnitTests;

public sealed class PipelineDefinitionTests
{
    [Fact]
    public async Task DescribeWhenNamedStepsConfiguredReturnsGraphAndExports()
    {
        // Arrange
        var pipeline = Pipeline
            .For<string>()
            .Then("Parse", value => int.Parse(value))
            .ThenAsync("Increment", (value, _) => ValueTask.FromResult(value + 1))
            .Build("Named pipeline");

        // Act
        int result = await pipeline.RunAsync("41");
        var definition = pipeline.Describe();

        // Assert
        Assert.Equal(42, result);
        Assert.Equal(pipeline.Id, definition.Id);
        Assert.Equal("Named pipeline", definition.Name);

        var parseNode = Assert.Single(definition.Nodes, node => node.Name == "Parse");
        Assert.Equal(typeof(string), parseNode.InputType);
        Assert.Equal(typeof(int), parseNode.OutputType);
        Assert.Equal(PipelineNodeKind.Step, parseNode.Kind);

        Assert.Contains(definition.Edges, edge => edge.From == "input" && edge.To == parseNode.Id);
        Assert.Contains("Parse", definition.ToMermaid());
        Assert.Contains("digraph", definition.ToDot());
        Assert.Contains("\"name\": \"Parse\"", definition.ToJson());
    }

    [Fact]
    public void DescribeWhenBranchConfiguredIncludesBranchPaths()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Branch(
                "Sign route",
                value => value >= 0,
                value => value,
                _ => 0)
            .Build();

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Contains(definition.Nodes, node => node.Name == "Sign route" && node.Kind == PipelineNodeKind.Branch);
        Assert.Contains(definition.Nodes, node => node.Name == "Sign route: true" && node.Kind == PipelineNodeKind.BranchPath);
        Assert.Contains(definition.Nodes, node => node.Name == "Sign route: false" && node.Kind == PipelineNodeKind.BranchPath);
        Assert.Contains(definition.Edges, edge => edge.Label == "true");
        Assert.Contains(definition.Edges, edge => edge.Label == "false");
    }

    [Fact]
    public void DescribeWhenForkAndMergeConfiguredIncludesParallelShape()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Fork(
                "Fan out",
                static (value, _) => ValueTask.FromResult(value + 1),
                static (value, _) => ValueTask.FromResult(value + 2))
            .Merge<int, int>("Sum", (results, _) => ValueTask.FromResult(results.Sum()))
            .Build();

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Contains(definition.Nodes, node => node.Name == "Fan out" && node.Kind == PipelineNodeKind.Fork);
        Assert.Contains(definition.Nodes, node => node.Name == "Fan out: result" && node.Kind == PipelineNodeKind.ForkJoin);
        Assert.Contains(definition.Nodes, node => node.Name == "Sum" && node.Kind == PipelineNodeKind.Merge);
        Assert.Contains(definition.Edges, edge => edge.Label == "branch 0");
        Assert.Contains(definition.Edges, edge => edge.Label == "branch 1");
    }

    [Fact]
    public void DescribeWhenStreamPipelineConfiguredReturnsGraph()
    {
        // Arrange
        var pipeline = Pipeline
            .StreamFor<int>()
            .Then("Increment", value => value + 1)
            .Build("Stream definition");

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Equal(pipeline.Id, definition.Id);
        Assert.Equal("Stream definition", definition.Name);
        Assert.Contains(definition.Nodes, node => node.Name == "Increment" && node.Kind == PipelineNodeKind.Step);
    }
}