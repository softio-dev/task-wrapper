# Add Virtual Threads Support

## Summary

The library can support Java virtual threads without changing its producer/consumer programming model.

The current implementation is already structurally compatible:

- Producers and consumers are ordinary `Runnable` tasks.
- Workers communicate through `LinkedBlockingQueue`.
- `ExecutorCompletionService`, `Future`, interruption, and blocking queues work with virtual threads.
- The project targets Java 25, where virtual threads are stable and `synchronized` no longer causes the common carrier-thread pinning problem addressed by JEP 491.

The smallest possible change is replacing the fixed platform-thread executor in `TaskLifecycleRunner`:

```java
ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);
```

with a virtual-thread-per-task executor:

```java
ThreadFactory threadFactory = Thread.ofVirtual()
    .name("task-worker-", 0)
    .factory();

ExecutorService executorService =
    Executors.newThreadPerTaskExecutor(threadFactory);
```

However, virtual-thread support should be introduced as a configurable execution policy rather than as an isolated executor replacement.

## Expected Benefits

Virtual threads are useful when producer or consumer functions spend significant time blocked on:

- JDBC operations
- HTTP calls
- File operations
- Other blocking I/O
- Queue operations

They reduce the number of required platform threads and allow many task wrappers to run concurrently with lower thread-management cost.

Virtual threads do not:

- Make CPU-intensive work faster.
- Reduce the latency of individual operations.
- Increase database connection-pool capacity.
- Remove the need to limit concurrency against external services.
- Make mutable producer or consumer state automatically thread-safe.

Simply replacing the current executor will reduce platform-thread usage, but each task will still create only the configured number of producer and consumer workers.

## 1. Add a Configurable Worker Execution Policy

Do not hard-code virtual threads as the only execution mode initially. Preserve platform-thread execution and add virtual threads as an explicit option.

For example:

```java
public enum WorkerThreadType
{
  PLATFORM,
  VIRTUAL
}
```

Executor creation can then be centralized:

```java
private static ExecutorService createExecutor(
    final WorkerThreadType threadType,
    final int workerCount,
    final String taskName)
{
  if (threadType == WorkerThreadType.VIRTUAL) {
    ThreadFactory threadFactory = Thread.ofVirtual()
        .name(taskName + "-worker-", 0)
        .factory();

    return Executors.newThreadPerTaskExecutor(threadFactory);
  }

  return Executors.newFixedThreadPool(workerCount);
}
```

The thread type could be supplied through:

- Task-wrapper constructors.
- A task-options object.
- An executor factory.
- Spring Boot configuration, if the library later provides auto-configuration.

An executor factory is preferable to accepting a shared `ExecutorService`. Every task currently owns and shuts down its executor. Shutting down an externally supplied shared executor would be incorrect unless executor ownership were represented explicitly.

Do not create a fixed-size pool of virtual threads. A virtual thread should represent a task, and the JDK virtual-thread-per-task executor already implements that model.

## 2. Preserve Explicit Concurrency Limits

Virtual threads are inexpensive, but downstream resources are not.

The existing consumer count should continue to limit concurrent processing:

```java
task.addConsumers(consumer, 10);
```

This should still mean that no more than ten consumer callbacks run concurrently.

If a future design creates one virtual thread per queue item, it must introduce an explicit concurrency limit, such as a `Semaphore`. A fixed virtual-thread pool should not be used for throttling.

Database connection pools already provide an effective concurrency limit. An additional semaphore is normally unnecessary unless the application needs a stricter limit than the connection pool.

## 3. Replace Polling with Blocking Queue Operations

The current producer and consumer loops use short polling intervals:

```java
queue.offer(item, 10, TimeUnit.MILLISECONDS);
queue.poll(10, TimeUnit.MILLISECONDS);
```

`BatchConsumer` uses `drainTo()` and sleeps for one millisecond when the queue is empty.

These operations are compatible with virtual threads, so they do not need to change in the first implementation. However, they cause repeated wake-ups and complicate the control flow.

Virtual threads work well with straightforward blocking operations:

```java
queue.put(item);
T item = queue.take();
```

When these operations block, the virtual thread can normally unmount from its carrier thread. This allows the carrier to execute another virtual thread.

Changing to indefinite blocking requires active cancellation and an explicit producer-completion protocol. Workers would no longer wake every ten milliseconds to inspect `queueWrapper.isInterrupt()`.

## 4. Add Active Worker Cancellation

Currently, `TaskWrapper.interrupt()` only sets the queue wrapper's atomic interruption flag. Workers observe that flag because their queue operations use short timeouts.

For workers blocked in `put()` or `take()`, interruption must also reach the worker threads.

The lifecycle implementation should:

1. Retain the `Future<?>` returned for every submitted producer and consumer.
2. Store an active execution handle that can be reached by the task wrapper.
3. Set the task-level interruption flag when cancellation is requested.
4. Call `cancel(true)` on every worker future.
5. Continue using `shutdownNow()` during final cleanup.
6. Preserve the interrupted status when catching `InterruptedException`.

Producer and consumer callbacks must also cooperate with interruption. A callback that catches and ignores `InterruptedException`, or performs non-interruptible blocking work indefinitely, cannot be stopped promptly by the library.

## 5. Add Explicit Queue Completion Signaling

Consumers currently determine completion by repeatedly checking:

```java
queueWrapper.isProducersCompleted() && queue.isEmpty()
```

That works with timed polling but does not wake a consumer blocked in `take()`.

A clean solution is to store queue entries rather than raw values:

```java
sealed interface QueueEntry<T>
{
  record Item<T>(T value) implements QueueEntry<T> {}

  record End<T>() implements QueueEntry<T> {}
}
```

After every producer has completed, the lifecycle runner can enqueue one `End` marker per active consumer. Each consumer exits after receiving an end marker.

Requirements for this approach:

- The end marker must never be exposed to user callbacks.
- There must be enough end markers for every active consumer.
- Consumer replacement logic must account for markers already consumed by failed workers.
- Cancellation must still interrupt workers rather than rely only on end markers.
- Queue capacity and completion-marker insertion must not cause a shutdown deadlock.

An alternative is to keep timed polling in the first virtual-thread release. This is less efficient but substantially reduces the initial migration risk.

## 6. Improve Batch Consumer Blocking

The batch consumer can block until at least one item is available, then drain any immediately available additional items:

```java
T first = queue.take();

List<T> batch = new ArrayList<>(batchSize);
batch.add(first);
queue.drainTo(batch, batchSize - 1);

consumer.accept(batch);
```

This removes the one-millisecond sleep and prevents empty polling loops.

Completion markers require additional handling so that:

- An end marker is not included in a user batch.
- Items that precede completion are processed.
- Multiple consumers terminate correctly.

## 7. Add an Asynchronous Execution Handle

The current `executeTask()` method is blocking, and README examples start it with:

```java
CompletableFuture.runAsync(task::executeTask);
```

Without an explicit executor, this normally uses the common `ForkJoinPool`, whose workers are platform threads.

For a direct virtual-thread start, callers can use:

```java
Thread.ofVirtual()
    .name(task.getTaskName())
    .start(task::executeTask);
```

A stronger library API would hide this detail:

```java
TaskExecution execution = task.start();

execution.cancel();
execution.await();
```

`TaskExecution` could expose:

- Task name
- Completion state
- Cancellation
- Blocking `await()`
- Timed `await(...)`
- Failure cause
- The coordinator thread or future

This would separate the task definition from a particular calling-thread strategy and provide a consistent cancellation API.

## 8. Thread Naming and Observability

Virtual threads have no name by default. Named thread factories should be used so logs and thread dumps identify:

- Task name
- Worker type
- Worker sequence

Possible names include:

```text
user-export-producer-0
user-export-consumer-0
user-export-consumer-1
```

The current executor submits wrapped callables to `ExecutorCompletionService`. If separate producer and consumer names are required, worker names can be assigned inside the submitted wrapper or through separate thread factories/executors.

Java 25 provides virtual-thread visibility through:

- `jcmd <pid> Thread.dump_to_file`
- Java Flight Recorder events
- `Thread.currentThread().isVirtual()`

Relevant JFR events include:

- `jdk.VirtualThreadStart`
- `jdk.VirtualThreadEnd`
- `jdk.VirtualThreadPinned`
- `jdk.VirtualThreadSubmitFailed`

## 9. Review Thread-Local Usage

The library itself does not currently use `ThreadLocal`.

User-supplied producer and consumer callbacks may use frameworks that do. Context values such as transaction or request identifiers are valid thread-local use cases, but caching large or expensive reusable objects in a `ThreadLocal` is a poor fit for virtual threads because each task receives a new thread.

Documentation should warn users to review:

- Thread-local object caches
- MDC/logging context propagation
- Security context propagation
- Transaction context
- Libraries that assume a small reusable platform-thread pool

## 10. Pinning Considerations on Java 25

The project targets Java 25.

JEP 491, delivered in Java 24, changed monitor handling so blocking inside `synchronized` methods and statements no longer causes the common virtual-thread pinning problem.

Remaining pinning risks primarily involve:

- Native methods
- Foreign-function calls
- Some class-loading and class-initialization situations

Pinning does not make execution incorrect, but long or frequent pinning can reduce scalability. Java Flight Recorder should be used to identify material pinning in representative workloads.

The old `jdk.tracePinnedThreads` system property is no longer useful on Java 25; JFR is the appropriate diagnostic mechanism.

## 11. Do Not Require Structured Concurrency Yet

Structured concurrency is relevant because a task wrapper owns a group of producer and consumer subtasks whose failures and cancellation are related.

However, `StructuredTaskScope` remains a preview API in Java 25 under JEP 505. Requiring it would force library users to compile and run with preview features enabled.

The initial virtual-thread implementation should continue using stable APIs:

- `ExecutorService`
- `ExecutorCompletionService`
- `Future`
- Thread interruption

Structured concurrency can be reconsidered after the API becomes final or as a separate preview-enabled module.

## 12. Testing Requirements

All lifecycle tests should run against both platform-thread and virtual-thread execution modes.

Add tests that verify:

- Producer callbacks run on virtual threads in virtual mode.
- Basic consumer callbacks run on virtual threads.
- Batch consumer callbacks run on virtual threads.
- Platform mode still uses platform threads.
- A producer blocked by a full queue can be interrupted.
- A consumer blocked by an empty queue can be interrupted.
- All consumers terminate after producer completion.
- Multiple producers complete before completion is signaled.
- Consumer restart creates a worker using the selected thread type.
- Consumer restart limits behave identically in both modes.
- Producer exceptions cancel consumers and clean up the queue.
- Consumer failures do not leak workers.
- Task registration and unregistration remain correct.
- The executor terminates after successful execution, failure, and cancellation.
- Queue contents are cleared during final cleanup.
- Thread names contain useful task and worker information.

Tests should use latches and bounded waits rather than arbitrary sleeps wherever possible.

## 13. Benchmarking

Benchmark before changing the default execution policy.

Useful scenarios include:

1. Blocking HTTP or JDBC-style consumers.
2. Many concurrently running task wrappers.
3. Slow consumers with a full bounded queue.
4. CPU-intensive consumers.
5. Small and large consumer counts.
6. Consumer failure and restart under load.
7. Cancellation while producers and consumers are blocked.

Expected result:

- Virtual threads should improve scalability when many workers spend time blocked.
- They may provide little benefit for the current small fixed worker counts.
- They should not improve CPU-bound throughput and can make excessive CPU concurrency easier to configure accidentally.

## Recommended Implementation Order

1. Introduce a configurable platform/virtual worker execution policy.
2. Create named virtual threads with `Executors.newThreadPerTaskExecutor(...)`.
3. Run the existing test suite in both execution modes.
4. Add virtual-thread identity, lifecycle, cancellation, and leak tests.
5. Add active cancellation by retaining and cancelling worker futures.
6. Replace timed producer polling with blocking `put()`.
7. Introduce a safe consumer-completion protocol.
8. Replace consumer polling and batch sleeping with blocking `take()` plus `drainTo()`.
9. Add a `start()` method that returns a task execution handle.
10. Benchmark realistic blocking and CPU-bound workloads.
11. Decide whether virtual threads should become the default.

## Recommended Initial Scope

The safest first release should:

- Add `PLATFORM` and `VIRTUAL` execution modes.
- Keep the existing timed queue polling.
- Use a named virtual-thread-per-task executor in virtual mode.
- Preserve existing consumer-count concurrency limits.
- Add tests for both modes.
- Document that the main benefit appears with blocking producer and consumer functions.

Active cancellation and fully blocking queue operations should follow as a second change. Combining executor replacement, queue protocol changes, cancellation changes, and public API changes into one release would make lifecycle regressions harder to isolate.

## References

- [Oracle Java 25 Virtual Threads Guide](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html)
- [Java 25 `Executors` API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/Executors.html)
- [Java 25 `ExecutorService` API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/ExecutorService.html)
- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444)
- [JEP 491: Synchronize Virtual Threads without Pinning](https://openjdk.org/jeps/491)
- [JEP 505: Structured Concurrency (Fifth Preview)](https://openjdk.org/jeps/505)
