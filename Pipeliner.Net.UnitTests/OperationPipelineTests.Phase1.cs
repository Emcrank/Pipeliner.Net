namespace Pipeliner.Net.UnitTests;

public partial class OperationPipelineTests
{
    [Fact]
    public async Task RunBatchAsync_ReadOnlyMemory_Success()
    {
        // Arrange
        var pipeline = CreateStringIncrementPipelineBuilder().Build();
        ReadOnlyMemory<string> inputBatch = new[] { "10", "20", "30" };

        // Act
        var results = await pipeline.RunBatchAsync(inputBatch);

        // Assert
        Assert.Equal([15, 25, 35], results);
    }

    [Fact]
    public async Task RunBatchAsync_IAsyncEnumerable_Success()
    {
        // Arrange
        var pipeline = CreateStringIncrementPipelineBuilder().Build();

        // Act
        var results = new List<int>();
        await foreach (var result in pipeline.RunBatchAsync(GetBatchInputsAsync()))
        {
            if (result is int value)
                results.Add(value);
        }

        // Assert
        Assert.Equal([15, 25, 35], results);
    }

    [Fact]
    public async Task RunAsync_TypeThreadedBuilder_WithPolicyAndThenParallel_Success()
    {
        // Arrange
        var executionAttempts = 0;

        var pipeline = Pipeline
            .For<int[]>()
            .ThenAsync<int[]>(async (values, cancellationToken) =>
            {
                await Task.Yield();
                cancellationToken.ThrowIfCancellationRequested();
                executionAttempts++;

                if (executionAttempts < 2)
                    throw new InvalidOperationException("Simulated transient error.");

                return values;
            })
            .WithPolicy(new RetryExecutionPolicy(2))
            .ThenParallel<int, int>((value, _) => ValueTask.FromResult(value + IncrementByFive), ParallelStepOptions.Create(2))
            .Build();

        // Act
        var result = await pipeline.RunAsync([10, 20, 30]);

        // Assert
        Assert.Equal([15, 25, 35], result);
        Assert.Equal(2, executionAttempts);
    }

    [Fact]
    public void Run_TypeThreadedBuilder_WithFactoryStep_Success()
    {
        // Arrange
        var pipeline = Pipeline
            .For<string>()
            .Then<int>(Convert.ToInt32)
            .Then<AddFiveStep, int>(() => new AddFiveStep())
            .Build();

        // Act
        var result = pipeline.Run(ValidNumber);

        // Assert
        Assert.Equal(55, result);
    }

    private static async IAsyncEnumerable<string> GetBatchInputsAsync()
    {
        yield return "10";
        await Task.Yield();
        yield return "20";
        await Task.Yield();
        yield return "30";
    }

    private sealed class AddFiveStep : IPipelineStep<int, int>
    {
        public ValueTask<int> ExecuteAsync(int input, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(input + IncrementByFive);
        }
    }
}
