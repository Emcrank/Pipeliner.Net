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
    public async Task RunStreamAsyncWithBatchBySizeEmitsBatches()
    {
        // Arrange
        var pipeline = Pipeline
            .StreamFor<int>()
            .Batch(size: 2)
            .Build();

        // Act
        var results = new List<IReadOnlyList<int>>();
        await foreach (var batch in pipeline.RunStreamAsync(GetValuesAsync([1, 2, 3])))
            results.Add(batch);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal([1, 2], results[0]);
        Assert.Equal([3], results[1]);
    }

    [Fact]
    public async Task RunStreamAsyncWithBatchByDelayFlushesPartialBatch()
    {
        // Arrange
        var pipeline = Pipeline
            .StreamFor<int>()
            .Batch(size: 10, maxDelay: TimeSpan.FromMilliseconds(25))
            .Build();

        // Act
        var results = new List<IReadOnlyList<int>>();
        await foreach (var batch in pipeline.RunStreamAsync(GetDelayedPairAsync(TimeSpan.FromMilliseconds(75))))
            results.Add(batch);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal([1], results[0]);
        Assert.Equal([2], results[1]);
    }

    [Fact]
    public async Task RunStreamAsyncWithWindowEmitsTimeWindowedItems()
    {
        // Arrange
        var pipeline = Pipeline
            .StreamFor<int>()
            .Window(TimeSpan.FromMilliseconds(25))
            .Then(batch => batch.Sum())
            .Build();

        // Act
        var results = new List<int>();
        await foreach (var sum in pipeline.RunStreamAsync(GetDelayedPairAsync(TimeSpan.FromMilliseconds(75))))
            results.Add(sum);

        // Assert
        Assert.Equal([1, 2], results);
    }

    [Fact]
    public void BatchWhenSizeIsInvalidThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var builder = Pipeline.StreamFor<int>();

        // Act + Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Batch(0));
    }

    [Fact]
    public void WindowWhenDurationIsInvalidThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var builder = Pipeline.StreamFor<int>();

        // Act + Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Window(TimeSpan.Zero));
    }

    [Fact]
    public void WithBackpressureWhenCapacityIsInvalidThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var builder = Pipeline.StreamFor<int>();

        // Act + Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithBackpressure(new BackpressureOptions(0, BackpressureMode.Wait)));
    }

    private static async IAsyncEnumerable<int> GetDelayedPairAsync(
        TimeSpan delay,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return 1;
        await Task.Delay(delay, cancellationToken);
        yield return 2;
    }

    private static async IAsyncEnumerable<int> GetValuesAsync(
        IEnumerable<int> values,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var value in values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return value;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> GetDelayedValuesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
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