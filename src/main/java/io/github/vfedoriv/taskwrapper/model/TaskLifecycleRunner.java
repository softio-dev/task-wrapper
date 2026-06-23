package io.github.vfedoriv.taskwrapper.model;

import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.github.vfedoriv.taskwrapper.consumer.BasicConsumer;
import io.github.vfedoriv.taskwrapper.consumer.BatchConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class TaskLifecycleRunner
{
  private enum WorkerType
  {
    PRODUCER,
    CONSUMER
  }

  private static final class WorkerFailedException
      extends RuntimeException
  {
    private final WorkerType workerType;

    private WorkerFailedException(final WorkerType workerType, final Throwable cause) {
      super(cause);
      this.workerType = workerType;
    }
  }

  private TaskLifecycleRunner() {
  }

  static void run(
      final List<? extends Runnable> producers,
      final List<? extends Runnable> consumers,
      final QueueWrapper<?> queueWrapper,
      final int maxConsumerRestarts)
  {
    int threadsCount = consumers.size() + producers.size();
    if (threadsCount == 0 || producers.isEmpty() || consumers.isEmpty()) {
      throw new IllegalStateException("At least one producer and one consumer are required");
    }
    if (maxConsumerRestarts < 0) {
      throw new IllegalArgumentException("Max consumer restarts must not be negative");
    }

    ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);
    try {
      CompletionService<WorkerType> completionService = new ExecutorCompletionService<>(executorService);
      producers.forEach(producer -> submitWorker(completionService, producer, WorkerType.PRODUCER));
      consumers.forEach(consumer -> submitWorker(completionService, consumer, WorkerType.CONSUMER));

      waitForWorkers(completionService, threadsCount, producers.size(), queueWrapper, maxConsumerRestarts, consumers);
      queueWrapper.consumersComplete();
      log.trace("consumers completed");
    }
    catch (RuntimeException e) {
      queueWrapper.interrupt();
      throw e;
    }
    finally {
      queueWrapper.interrupt();
      executorService.shutdownNow();
      awaitTermination(executorService);
      queueWrapper.clear();
    }
  }

  private static void waitForWorkers(
      final CompletionService<WorkerType> completionService,
      final int threadsCount,
      final int producersCount,
      final QueueWrapper<?> queueWrapper,
      final int maxConsumerRestarts,
      final List<? extends Runnable> initialConsumers)
  {
    int completedProducers = 0;
    boolean producersMarkedComplete = false;
    int expectedCompletions = threadsCount;
    int completedWorkers = 0;
    int consumerRestarts = 0;
    int nextConsumerIndex = 0;
    while (completedWorkers < expectedCompletions) {
      try {
        WorkerType workerType = completionService.take().get();
        completedWorkers++;
        if (workerType == WorkerType.PRODUCER) {
          completedProducers++;
          if (!producersMarkedComplete && completedProducers == producersCount) {
            queueWrapper.producersComplete();
            producersMarkedComplete = true;
            log.trace("producers completed");
          }
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Task execution was interrupted", e);
      }
      catch (ExecutionException e) {
        completedWorkers++;
        Throwable cause = e.getCause();
        if (isConsumerFailure(cause)) {
          if (consumerRestarts >= maxConsumerRestarts) {
            throw new IllegalStateException("Consumer restart limit exceeded", workerCause(cause));
          }
          Runnable replacement = newConsumerReplacement(initialConsumers.get(nextConsumerIndex));
          nextConsumerIndex = (nextConsumerIndex + 1) % initialConsumers.size();
          consumerRestarts++;
          expectedCompletions++;
          submitWorker(completionService, replacement, WorkerType.CONSUMER);
          continue;
        }
        throw new IllegalStateException("Task worker failed", workerCause(cause));
      }
    }
  }

  private static void submitWorker(
      final CompletionService<WorkerType> completionService,
      final Runnable worker,
      final WorkerType workerType)
  {
    completionService.submit(() -> {
      try {
        worker.run();
        return workerType;
      }
      catch (RuntimeException | Error e) {
        throw new WorkerFailedException(workerType, e);
      }
    });
  }

  private static boolean isConsumerFailure(final Throwable cause) {
    return cause instanceof WorkerFailedException workerFailedException
        && workerFailedException.workerType == WorkerType.CONSUMER;
  }

  private static Throwable workerCause(final Throwable cause) {
    return cause instanceof WorkerFailedException && cause.getCause() != null ? cause.getCause() : cause;
  }

  private static Runnable newConsumerReplacement(final Runnable consumer) {
    if (consumer instanceof BasicConsumer<?> basicConsumer) {
      return basicConsumer.newReplacement();
    }
    if (consumer instanceof BatchConsumer<?> batchConsumer) {
      return batchConsumer.newReplacement();
    }
    throw new IllegalStateException("Consumer replacement is not supported for " + consumer.getClass().getName());
  }

  private static void awaitTermination(final ExecutorService executorService) {
    try {
      if (!executorService.awaitTermination(1L, TimeUnit.SECONDS)) {
        log.warn("Task workers did not stop within cleanup timeout");
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
