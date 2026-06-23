package io.github.vfedoriv.taskwrapper.model;

import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
final class TaskLifecycleRunner
{
  private enum WorkerType
  {
    PRODUCER,
    CONSUMER
  }

  private TaskLifecycleRunner() {
  }

  static void run(
      final List<? extends Runnable> producers,
      final List<? extends Runnable> consumers,
      final QueueWrapper<?> queueWrapper)
  {
    int threadsCount = consumers.size() + producers.size();
    if (threadsCount == 0 || producers.isEmpty() || consumers.isEmpty()) {
      throw new IllegalStateException("At least one producer and one consumer are required");
    }

    ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);
    try {
      CompletionService<WorkerType> completionService = new ExecutorCompletionService<>(executorService);
      producers.forEach(producer -> completionService.submit(() -> {
        producer.run();
        return WorkerType.PRODUCER;
      }));
      consumers.forEach(consumer -> completionService.submit(() -> {
        consumer.run();
        return WorkerType.CONSUMER;
      }));

      waitForWorkers(completionService, threadsCount, producers.size(), queueWrapper);
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
    }
  }

  private static void waitForWorkers(
      final CompletionService<WorkerType> completionService,
      final int threadsCount,
      final int producersCount,
      final QueueWrapper<?> queueWrapper)
  {
    int completedProducers = 0;
    boolean producersMarkedComplete = false;
    for (int i = 0; i < threadsCount; i++) {
      try {
        WorkerType workerType = completionService.take().get();
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
        throw new IllegalStateException("Task worker failed", e.getCause());
      }
    }
  }
}
