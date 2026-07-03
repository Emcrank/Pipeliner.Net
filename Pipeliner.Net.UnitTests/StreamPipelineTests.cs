using System.Runtime.CompilerServices;

namespace Pipeliner.Net.UnitTests;

public sealed class StreamPipelineTests
{
    [Fact]
    public async Task RunStreamAsyncWhenItemsProvidedTransformsAllItems()
    {
        // Arrange
        var pipeline = Pipeline
            .StreamFor<int>()
            .Then<int>(value => value + 1)
            .WithBackpressure(BackpressureOptions.Create(4, BackpressureMode.Wait))
            .Build();

        // Act
        var results = new List<int>();
        await foreach (var item in pipeline.RunStreamAsync(GetValuesAsync([1, 2, 3])))
            results.Add(item);

        // Assert
        Assert.Equal([2, 3, 4], results);
    }

    [Fact]
    public async Task RunStreamAsyncWhenCancelledThrowsOperationCanceledException()
    {
        // Arrange
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(30));

        var pipeline = Pipeline
            .StreamFor<int>()
            .ThenAsync<int>(async (value, cancellationToken) =>
            {
                await Task.Delay(50, cancellationToken);
                return value + 1;
            })
            .Build();

        // Act + Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var item in pipeline.RunStreamAsync(GetDelayedValuesAsync(cancellationTokenSource.Token), cancellationTokenSource.Token))
                _ = item;
        });
    }

    [Fact]
    public void WithBackpressureWhenCapacityIsInvalidThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var builder = Pipeline.StreamFor<int>();

        // Act + Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBackpressure(new BackpressureOptions(0, BackpressureMode.Wait)));
    }

    private static async IAsyncEnumerable<int> GetValuesAsync(IEnumerable<int> values, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var value in values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return value;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> GetDelayedValuesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var value = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(10, cancellationToken);
            yield return value++;
        }
    }
}
