namespace Pipeliner.Net.UnitTests;

public sealed class PipelineCheckpointTests
{
    [Fact]
    public async Task RunAsyncWithInMemoryCheckpointStorePersistsCheckpointPayload()
    {
        // Arrange
        var store = new InMemoryPipelineCheckpointStore();
        var pipeline = Pipeline
            .For<string>()
            .Then("Parse", int.Parse)
            .Checkpoint("After parse")
            .Then("Increment", value => value + 1)
            .WithCheckpointing(store)
            .Build("Checkpointed pipeline");

        // Act
        var result = await pipeline.RunAsync("41");
        var checkpoints = await store.LoadByPipelineAsync(pipeline.Id);

        // Assert
        Assert.Equal(42, result);
        var checkpoint = Assert.Single(checkpoints);
        Assert.Equal("After parse", checkpoint.CheckpointName);
        Assert.Equal("41", checkpoint.PayloadJson);
        Assert.Equal(typeof(int).AssemblyQualifiedName, checkpoint.PayloadType);
    }

    [Fact]
    public async Task RunAsyncWithFileCheckpointStorePersistsCheckpointFile()
    {
        // Arrange
        var directory = Path.Combine(Path.GetTempPath(), $"pipeliner-checkpoints-{Guid.NewGuid():N}");
        var store = new FilePipelineCheckpointStore(directory);
        var pipeline = Pipeline
            .For<int>()
            .Then("Double", value => value * 2)
            .Checkpoint("After double")
            .WithCheckpointing(store)
            .Build();

        try
        {
            // Act
            var result = await pipeline.RunAsync(21);
            var checkpoints = await store.LoadByPipelineAsync(pipeline.Id);

            // Assert
            Assert.Equal(42, result);
            var checkpoint = Assert.Single(checkpoints);
            Assert.Equal("42", checkpoint.PayloadJson);
            Assert.True(Directory.EnumerateFiles(directory, "*.json").Any());
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void DescribeWhenCheckpointConfiguredIncludesCheckpointNode()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Then("Double", value => value * 2)
            .Checkpoint("After double")
            .Build();

        // Act
        var definition = pipeline.Describe();

        // Assert
        Assert.Contains(
            definition.Nodes,
            node => node.Name == "After double" && node.Kind == PipelineNodeKind.Checkpoint);
    }

    [Fact]
    public async Task RunAsyncWhenCheckpointPersistenceFailsFailsRunByDefault()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Checkpoint("Failure point")
            .WithCheckpointing(new ThrowingCheckpointStore())
            .Build();

        // Act + Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.RunAsync(42));
    }

    [Fact]
    public async Task RunAsyncWhenCheckpointPersistenceFailsCanContinue()
    {
        // Arrange
        var pipeline = Pipeline
            .For<int>()
            .Checkpoint("Failure point")
            .WithCheckpointing(
                new ThrowingCheckpointStore(),
                PipelineCheckpointFailureBehavior.Continue)
            .Build();

        // Act
        var result = await pipeline.RunAsync(42);

        // Assert
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task RunAsyncWithStatefulPipelinePersistsCheckpointPayload()
    {
        // Arrange
        var store = new InMemoryPipelineCheckpointStore();
        var pipeline = Pipeline
            .For<int>()
            .WithState(() => new CounterState())
            .Then("Use state", (value, state) =>
            {
                state.Count++;
                return value + state.Count;
            })
            .Checkpoint("After state")
            .WithCheckpointing(store)
            .Build("Stateful checkpoint pipeline");

        // Act
        var result = await pipeline.RunAsync(41);
        var checkpoints = await store.LoadByPipelineAsync(pipeline.Id);

        // Assert
        Assert.Equal(42, result);
        var checkpoint = Assert.Single(checkpoints);
        Assert.Equal("After state", checkpoint.CheckpointName);
        Assert.Equal("42", checkpoint.PayloadJson);
    }

    private sealed class CounterState
    {
        public int Count { get; set; }
    }

    private sealed class ThrowingCheckpointStore : IPipelineCheckpointStore
    {
        public ValueTask SaveAsync(PipelineCheckpoint checkpoint, CancellationToken cancellationToken = default) =>
            ValueTask.FromException(new InvalidOperationException("checkpoint unavailable"));

        public ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadAsync(
            string runId,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<IReadOnlyList<PipelineCheckpoint>>([]);

        public ValueTask<IReadOnlyList<PipelineCheckpoint>> LoadByPipelineAsync(
            string pipelineId,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<IReadOnlyList<PipelineCheckpoint>>([]);
    }
}