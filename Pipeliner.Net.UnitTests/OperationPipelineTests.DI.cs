using System.Collections.Generic;

namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    [Fact]
    public void Run_ServiceProviderResolvedOperation_Success()
    {
        // Arrange
        var serviceProvider = new TestServiceProvider(new Dictionary<Type, object>
        {
            [typeof(IncrementService)] = new IncrementService()
        });

        var pipeline = Pipeline
            .For<string>()
            .Then<int>(Convert.ToInt32)
            .ThenAsync<int>((value, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var service = serviceProvider.GetService(typeof(IncrementService));
                if (service is not IncrementService incrementService)
                    throw new InvalidOperationException($"Service `{typeof(IncrementService).FullName}` could not be resolved from the provided {nameof(IServiceProvider)}.");

                return ValueTask.FromResult(incrementService.Increment(value));
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
        var serviceProvider = new TestServiceProvider(new Dictionary<Type, object>
        {
            [typeof(IncrementService)] = new IncrementService()
        });

        var pipeline = Pipeline
            .For<string>()
            .Then<int>(Convert.ToInt32)
            .ThenAsync<int>((value, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var service = serviceProvider.GetService(typeof(IncrementService));
                if (service is not IncrementService incrementService)
                    throw new InvalidOperationException($"Service `{typeof(IncrementService).FullName}` could not be resolved from the provided {nameof(IServiceProvider)}.");

                return ValueTask.FromResult(incrementService.Increment(value));
            })
            .Build();

        // Act
        int result = await pipeline.RunAsync("50");

        // Assert
        Assert.Equal(55, result);
    }

    [Fact]
    public void Run_ServiceProviderMissingService_ThrowsInvalidOperationException()
    {
        // Arrange
        var serviceProvider = new TestServiceProvider(new Dictionary<Type, object>());

        var pipeline = Pipeline
            .For<string>()
            .Then<int>(Convert.ToInt32)
            .ThenAsync<int>((value, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var service = serviceProvider.GetService(typeof(IncrementService));
                if (service is not IncrementService incrementService)
                    throw new InvalidOperationException($"Service `{typeof(IncrementService).FullName}` could not be resolved from the provided {nameof(IServiceProvider)}.");

                return ValueTask.FromResult(incrementService.Increment(value));
            })
            .Build();

        // Act + Assert
        Action runAction = () => _ = pipeline.Run("50");
        Assert.Throws<InvalidOperationException>(runAction);
    }

    private sealed class IncrementService
    {
        public int Increment(int value) => value + 5;
    }

    private sealed class TestServiceProvider(IDictionary<Type, object> services) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            services.TryGetValue(serviceType, out var service) ? service : null;
    }
}
