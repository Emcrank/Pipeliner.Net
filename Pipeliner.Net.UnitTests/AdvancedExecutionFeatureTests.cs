using System.Collections.Concurrent;

namespace Pipeliner.Net.UnitTests;

public sealed class AdvancedExecutionFeatureTests
{
    [Fact]
    public async Task ThenAsync_ContextAwareStep_ReceivesStepScopedMetadata()
    {
        PipelineExecutionContext? captured = null;

        var pipeline = Pipeline
            .For<int>()
            .ThenAsync("double", (value, context, _) =>
            {
                captured = context;
                return ValueTask.FromResult(value * 2);
            })
            .Build("Context Pipeline", "2.4.0");

        var result = await pipeline.RunAsync(21);

        Assert.Equal(42, result);
        Assert.NotNull(captured);
        Assert.Equal("Context Pipeline", captured.PipelineName);
        Assert.Equal("2.4.0", captured.PipelineVersion);
        Assert.Equal("double", captured.StepName);
        Assert.Equal(PipelineNodeKind.Step, captured.StepKind);
        Assert.Equal(1, captured.AttemptNumber);
        Assert.False(string.IsNullOrWhiteSpace(captured.RunId));
    }

    [Fact]
    public async Task HaltWhen_PredicateIsTrue_HaltsRunAndNotifiesObserver()
    {
        var observer = new RecordingPipelineObserver();
        var executedAfterHalt = false;

        var pipeline = Pipeline
            .For<int>()
            .WithObserver(observer)
            .HaltWhen("manual-review", value => value > 10)
            .Then(value =>
            {
                executedAfterHalt = true;
                return value;
            })
            .Build("Halt Pipeline", "2.4.0");

        var exception = await Assert.ThrowsAsync<PipelineHaltedException>(() => pipeline.RunAsync(11));

        Assert.False(executedAfterHalt);
        Assert.Equal("manual-review", exception.HaltName);
        Assert.Contains(observer.RunHalted, halted => halted.HaltName == "manual-review");
        Assert.Contains(observer.StepHalted, halted => halted.Attempt.StepName == "manual-review");
    }

    [Fact]
    public async Task RunWithStatusAsync_WhenPipelineCompletes_ReturnsCompletedOutcome()
    {
        var pipeline = Pipeline
            .For<int>()
            .Then("double", value => value * 2)
            .Build("Status Pipeline", "2.4.0");

        var outcome = await pipeline.RunWithStatusAsync(21);

        Assert.True(outcome.IsCompleted);
        Assert.Equal(PipelineRunStatus.Completed, outcome.Status);
        Assert.Equal(42, outcome.Value);
        Assert.Null(outcome.Halt);
        Assert.Null(outcome.Exception);
    }

    [Fact]
    public async Task RunWithStatusAsync_WhenPipelineHalts_ReturnsHaltedOutcomeWithoutThrowing()
    {
        var pipeline = Pipeline
            .For<int>()
            .HaltWhen("manual-review", value => value > 10)
            .Then(value => value + 1)
            .Build("Halt Status Pipeline", "2.4.0");

        var outcome = await pipeline.RunWithStatusAsync(11);

        Assert.True(outcome.IsHalted);
        Assert.Equal(PipelineRunStatus.Halted, outcome.Status);
        Assert.Null(outcome.Exception);
        Assert.NotNull(outcome.Halt);
        Assert.Equal("manual-review", outcome.Halt.HaltName);
        Assert.Equal(pipeline.Id, outcome.Halt.PipelineId);
        Assert.Equal("Halt Status Pipeline", outcome.Halt.PipelineName);
        Assert.Equal("2.4.0", outcome.Halt.PipelineVersion);
    }

    [Fact]
    public async Task RunWithStatusAsync_WhenPipelineFails_ReturnsFailedOutcomeWithoutThrowing()
    {
        var pipeline = Pipeline
            .For<int>()
            .Then<int>("boom", _ => throw new InvalidOperationException("failed"))
            .Build("Failure Status Pipeline", "2.4.0");

        var outcome = await pipeline.RunWithStatusAsync(1);

        Assert.True(outcome.IsFailed);
        Assert.Equal(PipelineRunStatus.Failed, outcome.Status);
        Assert.Null(outcome.Halt);
        Assert.IsType<InvalidOperationException>(outcome.Exception);
    }
    [Fact]
    public async Task Observer_RecordsRunStepAndFailureAttempts()
    {
        var observer = new RecordingPipelineObserver();

        var pipeline = Pipeline
            .For<int>()
            .WithObserver(observer)
            .Then("ok", value => value + 1)
            .Then<int>("boom", _ => throw new InvalidOperationException("failed"))
            .Build("Observer Pipeline", "2.4.0");

        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.RunAsync(1));

        Assert.Single(observer.RunStarted);
        Assert.Single(observer.RunFailed);
        Assert.Contains(observer.StepCompleted, completed => completed.Attempt.StepName == "ok");
        Assert.Contains(observer.StepFailed, failed =>
            failed.Attempt.StepName == "boom" &&
            failed.Attempt.Status == PipelineStepAttemptStatus.Failed &&
            failed.Attempt.AttemptNumber == 1 &&
            failed.Attempt.ExceptionType == typeof(InvalidOperationException).FullName);
    }

    [Fact]
    public async Task Checkpoint_SavesPipelineVersionAndValidatesCompatibility()
    {
        var store = new InMemoryPipelineCheckpointStore();
        var pipeline = Pipeline
            .For<int>()
            .Then("plus-one", value => value + 1)
            .WithCheckpointing(store)
            .Checkpoint("after-plus-one")
            .Build("Checkpoint Pipeline", "2.4.0");

        var result = await pipeline.RunAsync(10);
        var checkpoints = await store.LoadByPipelineAsync(pipeline.Id);
        var checkpoint = Assert.Single(checkpoints);
        var compatible = pipeline.Describe().ValidateCheckpointCompatibility(checkpoint);

        Assert.Equal(11, result);
        Assert.Equal("2.4.0", checkpoint.PipelineVersion);
        Assert.True(compatible.IsCompatible, string.Join("; ", compatible.Issues));

        var incompatibleDefinition = new PipelineDefinition(
            pipeline.Id,
            pipeline.Name,
            pipeline.Describe().Nodes,
            pipeline.Describe().Edges,
            "3.0.0");

        var incompatible = incompatibleDefinition.ValidateCheckpointCompatibility(checkpoint);
        Assert.False(incompatible.IsCompatible);
        Assert.Contains(incompatible.Issues, issue => issue.Contains("version", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Describe_IncludesPipelineVersionInStructuredExports()
    {
        var pipeline = Pipeline
            .For<string>()
            .Then("parse", int.Parse)
            .Build("Versioned Pipeline", "2.4.0");

        var definition = pipeline.Describe();
        var json = definition.ToJson();

        Assert.Equal("2.4.0", pipeline.Version);
        Assert.Equal("2.4.0", definition.Version);
        Assert.Contains("\"version\": \"2.4.0\"", json);
    }

    private sealed class RecordingPipelineObserver : IPipelineObserver
    {
        public ConcurrentBag<PipelineRunStarted> RunStarted { get; } = [];

        public ConcurrentBag<PipelineRunCompleted> RunCompleted { get; } = [];

        public ConcurrentBag<PipelineRunFailed> RunFailed { get; } = [];

        public ConcurrentBag<PipelineRunHalted> RunHalted { get; } = [];

        public ConcurrentBag<PipelineStepCompleted> StepCompleted { get; } = [];

        public ConcurrentBag<PipelineStepFailed> StepFailed { get; } = [];

        public ConcurrentBag<PipelineStepHalted> StepHalted { get; } = [];

        public ValueTask OnRunStartedAsync(PipelineRunStarted started, CancellationToken cancellationToken = default)
        {
            RunStarted.Add(started);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnRunCompletedAsync(PipelineRunCompleted completed, CancellationToken cancellationToken = default)
        {
            RunCompleted.Add(completed);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnRunFailedAsync(PipelineRunFailed failed, CancellationToken cancellationToken = default)
        {
            RunFailed.Add(failed);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnRunHaltedAsync(PipelineRunHalted halted, CancellationToken cancellationToken = default)
        {
            RunHalted.Add(halted);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnStepCompletedAsync(PipelineStepCompleted completed, CancellationToken cancellationToken = default)
        {
            StepCompleted.Add(completed);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnStepFailedAsync(PipelineStepFailed failed, CancellationToken cancellationToken = default)
        {
            StepFailed.Add(failed);
            return ValueTask.CompletedTask;
        }

        public ValueTask OnStepHaltedAsync(PipelineStepHalted halted, CancellationToken cancellationToken = default)
        {
            StepHalted.Add(halted);
            return ValueTask.CompletedTask;
        }
    }
}