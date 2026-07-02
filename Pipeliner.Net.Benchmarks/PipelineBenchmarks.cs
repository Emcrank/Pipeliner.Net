using BenchmarkDotNet.Attributes;

namespace Pipeliner.Net.Benchmarks;

[MemoryDiagnoser]
public class PipelineBenchmarks
{
    private readonly OperationPipeline<string, int> requestPipeline = Pipeline
        .For<string>()
        .Then<int>(Convert.ToInt32)
        .Then<int>(value => value + 5)
        .Build();

    private readonly ReadOnlyMemory<string> batchInput = new[] { "10", "20", "30", "40", "50" };

    [Benchmark]
    public int Run() => requestPipeline.Run("50");

    [Benchmark]
    public async Task<int> RunAsync() => await requestPipeline.RunAsync("50");

    [Benchmark]
    public async Task<IReadOnlyList<int>> RunBatchReadOnlyMemory() => await requestPipeline.RunBatchAsync(batchInput);
}
