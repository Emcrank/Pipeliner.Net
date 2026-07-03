namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    private sealed class IncrementService
    {
        public static int Increment(int value) => value + 5;
    }

    private sealed class TestServiceProvider(IDictionary<Type, object> services) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            services.TryGetValue(serviceType, out object? service) ? service : null;
    }

    [Fact]
    public void Run_ServiceProviderMissingService_ThrowsInvalidOperationException()
    {
        // Arrange
        var serviceProvider = new TestServiceProvider(new Dictionary<Type, object>());

        var pipeline = Pipeline
            .For<string>()
            .Then(Convert.ToInt32)
            .ThenAsync((value, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                object? service = serviceProvider.GetService(typeof(IncrementService));
                if (service is not IncrementService)
                    throw new InvalidOperationException(
                        $"Service `{typeof(IncrementService).FullName}` could not be resolved from the provided {nameof(IServiceProvider)}.");

                return ValueTask.FromResult(IncrementService.Increment(value));
            })
            .Build();

        // Act + Assert
        Action runAction = () => _ = pipeline.Run("50");
        Assert.Throws<InvalidOperationException>(runAction);
    }

    [Fact]
    public void Run_ServiceProviderResolvedOperation_Success()
    {
        // Arrange
        var serviceProvider = new TestServiceProvider(
            new Dictionary<Type, object>
            {
                [typeof(IncrementService)] = new IncrementService()
            });

        var pipeline = Pipeline
            .For<string>()
            .Then(Convert.ToInt32)
            .ThenAsync((value, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                object? service = serviceProvider.GetService(typeof(IncrementService));
                if (service is not IncrementService)
                    throw new InvalidOperationException(
                        $"Service `{typeof(IncrementService).FullName}` could not be resolved from the provided {nameof(IServiceProvider)}.");

                return ValueTask.FromResult(IncrementService.Increment(value));
            })
            .Build();

        // Act
        int result = pipeline.Run("50");

        // Assert
        Assert.Equal(55, result);
    }

    [Fact]
    public async Task RunAsync_ServiceProviderResolvedOperation_Success()
    {
        // Arrange
        var serviceProvider = new TestServiceProvider(
            new Dictionary<Type, object>
            {
                [typeof(IncrementService)] = new IncrementService()
            });

        var pipeline = Pipeline
            .For<string>()
            .Then(Convert.ToInt32)
            .ThenAsync((value, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                object? service = serviceProvider.GetService(typeof(IncrementService));
                if (service is not IncrementService)
                    throw new InvalidOperationException(
                        $"Service `{typeof(IncrementService).FullName}` could not be resolved from the provided {nameof(IServiceProvider)}.");

                return ValueTask.FromResult(IncrementService.Increment(value));
            })
            .Build();

        // Act
        int result = await pipeline.RunAsync("50");

        // Assert
        Assert.Equal(55, result);
    }
}