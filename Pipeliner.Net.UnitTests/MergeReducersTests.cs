namespace Pipeliner.Net.UnitTests;

public sealed class MergeReducersTests
{
    [Fact]
    public async Task ThrowOnAnyFailureAsyncWhenFailureExistsThrowsAggregateException()
    {
        // Arrange
        IReadOnlyList<ForkResult<int>> results =
        [
            new(0, true, 10, null),
            new(1, false, default, new InvalidOperationException("boom"))
        ];

        // Act + Assert
        await Assert.ThrowsAsync<AggregateException>(async () =>
            await MergeReducers.ThrowOnAnyFailureAsync(results));
    }

    [Fact]
    public async Task IgnoreFailuresAsyncWhenFailuresExistReturnsSuccessfulValuesOnly()
    {
        // Arrange
        IReadOnlyList<ForkResult<int>> results =
        [
            new(0, true, 10, null),
            new(1, false, default, new InvalidOperationException("boom")),
            new(2, true, 13, null)
        ];

        // Act
        var successfulResults = await MergeReducers.IgnoreFailuresAsync(results);

        // Assert
        Assert.Equal([10, 13], successfulResults);
    }

    [Fact]
    public async Task TakeFirstAsyncWhenSuccessExistsReturnsFirstSuccessfulValue()
    {
        // Arrange
        IReadOnlyList<ForkResult<int>> results =
        [
            new(0, false, default, new InvalidOperationException("boom")),
            new(1, true, 11, null),
            new(2, true, 13, null)
        ];

        // Act
        var first = await MergeReducers.TakeFirstAsync(results);

        // Assert
        Assert.Equal(11, first);
    }

    [Fact]
    public async Task ReduceAsyncWhenSuccessExistsAggregatesSuccessfulValues()
    {
        // Arrange
        IReadOnlyList<ForkResult<int>> results =
        [
            new(0, true, 11, null),
            new(1, false, default, new InvalidOperationException("boom")),
            new(2, true, 13, null)
        ];

        // Act
        var sum = await MergeReducers.ReduceAsync(
            results,
            0,
            (accumulate, value, _) => ValueTask.FromResult(accumulate + value));

        // Assert
        Assert.Equal(24, sum);
    }
}
