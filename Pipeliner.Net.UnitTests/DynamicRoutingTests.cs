namespace Pipeliner.Net.UnitTests;

public sealed class DynamicRoutingTests
{
    [Fact]
    public async Task RunAsyncRouteByWhenKeyMatchesRunsSelectedRoute()
    {
        // Arrange
        var pipeline = Pipeline
            .For<RouteInput>()
            .RouteBy<RouteKind, string>(
                "Route by kind",
                input => input.Kind,
                routes => routes
                    .When(RouteKind.A, input => $"a:{input.Value}")
                    .When(RouteKind.B, input => $"b:{input.Value}"))
            .Build();

        // Act
        var result = await pipeline.RunAsync(new RouteInput(RouteKind.B, 42));

        // Assert
        Assert.Equal("b:42", result);
    }

    [Fact]
    public async Task RunAsyncRouteByWhenNoKeyMatchesRunsDefaultRoute()
    {
        // Arrange
        var pipeline = Pipeline
            .For<RouteInput>()
            .RouteBy<RouteKind, string>(
                input => input.Kind,
                routes => routes
                    .When(RouteKind.A, input => $"a:{input.Value}")
                    .Default(input => $"default:{input.Value}"))
            .Build();

        // Act
        var result = await pipeline.RunAsync(new RouteInput(RouteKind.B, 7));

        // Assert
        Assert.Equal("default:7", result);
    }

    [Fact]
    public async Task RunAsyncRouteByWhenNoKeyMatchesAndNoDefaultThrowsPipelineRouteNotFoundException()
    {
        // Arrange
        var pipeline = Pipeline
            .For<RouteInput>()
            .RouteBy<RouteKind, string>(
                input => input.Kind,
                routes => routes.When(RouteKind.A, input => $"a:{input.Value}"))
            .Build();

        // Act
        var exception = await Assert.ThrowsAsync<PipelineRouteNotFoundException>(() =>
            pipeline.RunAsync(new RouteInput(RouteKind.B, 7)));

        // Assert
        Assert.Equal(RouteKind.B, exception.RouteKey);
    }

    [Fact]
    public void DescribeWhenRouteByConfiguredIncludesRouteNode()
    {
        // Arrange
        var pipeline = Pipeline
            .For<RouteInput>()
            .RouteBy<RouteKind, string>(
                "Route by kind",
                input => input.Kind,
                routes => routes.Default(input => input.Value.ToString()))
            .Build();

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Contains(definition.Nodes, node => node.Name == "Route by kind" && node.Kind == PipelineNodeKind.Route);
    }

    [Fact]
    public async Task RunWithTraceAsyncRouteByCapturesRouteTrace()
    {
        // Arrange
        var pipeline = Pipeline
            .For<RouteInput>()
            .RouteBy<RouteKind, string>(
                "Route by kind",
                input => input.Kind,
                routes => routes.Default(input => input.Value.ToString()))
            .Build();

        // Act
        var result = await pipeline.RunWithTraceAsync(new RouteInput(RouteKind.B, 99));

        // Assert
        Assert.Equal("99", result.Result);
        var trace = Assert.Single(result.Trace.Steps);
        Assert.Equal("Route by kind", trace.Name);
        Assert.Equal(PipelineNodeKind.Route, trace.Kind);
    }

    private enum RouteKind
    {
        A,
        B
    }

    private sealed record RouteInput(RouteKind Kind, int Value);
}