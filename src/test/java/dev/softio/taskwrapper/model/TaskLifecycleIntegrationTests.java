package dev.softio.taskwrapper.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import dev.softio.taskwrapper.producer.ProducerPageDTO;
import dev.softio.taskwrapper.service.TasksService;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TaskLifecycleIntegrationTests
{
  private static final Logger log = LoggerFactory.getLogger(TaskLifecycleIntegrationTests.class);

  private static final int DEFAULT_QUEUE_SIZE = 20;

  @Test
  void backpressureFillsBoundedQueueWhenConsumerIsSlowerAndThenDrains() throws Exception {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("backpressure", DEFAULT_QUEUE_SIZE, tasksService);
    List<Integer> consumed = new CopyOnWriteArrayList<>();
    CountDownLatch consumerStarted = new CountDownLatch(1);
    CountDownLatch releaseConsumer = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    wrapper.addProducer(new ProducerPageDTO<>(), pagedProducer(numberedItems(DEFAULT_QUEUE_SIZE + 5), DEFAULT_QUEUE_SIZE + 5));
    wrapper.addConsumer(item -> {
      consumerStarted.countDown();
      await(releaseConsumer);
      consumed.add(item);
    });

    Thread taskThread = startTask(wrapper, failure);
    assertTrue(consumerStarted.await(1L, TimeUnit.SECONDS));
    waitForQueueSize(wrapper, DEFAULT_QUEUE_SIZE);
    assertTrue(taskThread.isAlive());

    releaseConsumer.countDown();
    taskThread.join(2_000L);

    assertFalse(taskThread.isAlive());
    assertNoFailure(failure);
    assertEquals(numberedItems(DEFAULT_QUEUE_SIZE + 5), consumed.stream().sorted().toList());
    assertTrue(wrapper.isTaskCompleted());
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.getQueueWrapper().isEmpty());
  }

  @Test
  void taskWaitsForConsumerInFlightAfterProducerCompletesAndQueueEmpties() throws Exception {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("in-flight", 1, tasksService);
    CountDownLatch consumerStarted = new CountDownLatch(1);
    CountDownLatch releaseConsumer = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    wrapper.addProducer(new ProducerPageDTO<>(), pagedProducer(List.of(1), 1));
    wrapper.addConsumer(item -> {
      consumerStarted.countDown();
      await(releaseConsumer);
    });

    Thread taskThread = startTask(wrapper, failure);
    assertTrue(consumerStarted.await(1L, TimeUnit.SECONDS));

    assertTrue(taskThread.isAlive());
    assertTrue(wrapper.getQueueWrapper().isEmpty());

    releaseConsumer.countDown();
    taskThread.join(2_000L);

    assertFalse(taskThread.isAlive());
    assertNoFailure(failure);
    assertTrue(wrapper.isTaskCompleted());
    assertFalse(tasksService.hasTask(wrapper));
  }

  @Test
  void normalCompletionCleansQueueRegistrationAndWorkerThreads() {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("cleanup", 2, tasksService);
    List<Thread> workerThreads = new CopyOnWriteArrayList<>();
    wrapper.addProducer(new ProducerPageDTO<>(), page -> {
      workerThreads.add(Thread.currentThread());
      page.setItems(List.of(1, 2, 3));
      page.setCompleted(true);
      return page;
    });
    wrapper.addConsumer(item -> workerThreads.add(Thread.currentThread()));

    assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask);

    assertTrue(wrapper.isProducersCompleted());
    assertTrue(wrapper.isConsumersCompleted());
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.getQueueWrapper().isEmpty());
    assertTrue(workerThreads.stream().allMatch(thread -> !thread.isAlive()));
  }

  @Test
  void interruptStopsTaskClearsQueueAndUnregisters() throws Exception {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("interrupt-lifecycle", 1, tasksService);
    List<Thread> workerThreads = new CopyOnWriteArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    wrapper.addProducer(new ProducerPageDTO<>(), page -> {
      workerThreads.add(Thread.currentThread());
      page.setItems(List.of(1));
      return page;
    });
    wrapper.addConsumer(item -> {
      workerThreads.add(Thread.currentThread());
      sleep(50L);
    });

    Thread taskThread = startTask(wrapper, failure);
    while (!tasksService.hasTask(wrapper)) {
      Thread.sleep(1L);
    }

    tasksService.interrupt(wrapper.getTaskName());
    taskThread.join(2_000L);

    assertFalse(taskThread.isAlive());
    assertNoFailure(failure);
    assertTrue(wrapper.isInterrupted());
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.getQueueWrapper().isEmpty());
    assertTrue(workerThreads.stream().allMatch(thread -> !thread.isAlive()));
  }

  @Test
  void interruptStopsTaskWithMultipleProducersAndConsumers() throws Exception {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("interrupt-many-workers", DEFAULT_QUEUE_SIZE, tasksService);
    List<Thread> producerThreads = new CopyOnWriteArrayList<>();
    List<Thread> consumerThreads = new CopyOnWriteArrayList<>();
    List<Integer> producedItems = new CopyOnWriteArrayList<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger nextProducedItem = new AtomicInteger(1);
    AtomicInteger nextConsumerNumber = new AtomicInteger(1);
    AtomicBoolean monitorRunning = new AtomicBoolean(true);

    wrapper.addProducer(new ProducerPageDTO<>(), sequencedProducer(1, wrapper, producerThreads, producedItems,
        nextProducedItem));
    wrapper.addProducer(new ProducerPageDTO<>(), sequencedProducer(2, wrapper, producerThreads, producedItems,
        nextProducedItem));
    wrapper.addConsumers(item -> {
      consumerThreads.add(Thread.currentThread());
      int consumerNumber = nextConsumerNumber.getAndUpdate(current -> current == 1 ? 2 : 1);
      log.info("consumer {} consumed item {}; current queue size {}", consumerNumber, item, wrapper.getQueueWrapper().size());
      sleep(50L);
    }, 2);

    Thread queueMonitor = startQueueMonitor(wrapper, monitorRunning);
    Thread taskThread = startTask(wrapper, failure);
    try {
      while (!tasksService.hasTask(wrapper) || producerThreads.size() < 2 || consumerThreads.size() < 2) {
        Thread.sleep(1L);
      }
      waitForQueueSize(wrapper, DEFAULT_QUEUE_SIZE);

      log.info("interrupt signal received; current queue size {}", wrapper.getQueueWrapper().size());
      tasksService.interrupt(wrapper.getTaskName());
      taskThread.join(2_000L);
    }
    finally {
      monitorRunning.set(false);
      queueMonitor.join(1_000L);
    }

    assertFalse(taskThread.isAlive());
    assertNoFailure(failure);
    assertTrue(wrapper.isInterrupted());
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.getQueueWrapper().isEmpty());
    assertTrue(producerThreads.stream().allMatch(thread -> !thread.isAlive()));
    assertTrue(consumerThreads.stream().allMatch(thread -> !thread.isAlive()));
    assertTrue(producedItems.containsAll(List.of(1, 2, 3, 4)));
  }

  @Test
  void transientConsumerFailureCreatesReplacementAndDrainsRemainingItems() {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("recover-consumer", 3, tasksService);
    wrapper.setMaxConsumerRestarts(1);
    AtomicInteger invocations = new AtomicInteger();
    List<Integer> consumed = new CopyOnWriteArrayList<>();
    wrapper.addProducer(new ProducerPageDTO<>(), pagedProducer(List.of(1, 2, 3), 3));
    wrapper.addConsumer(item -> {
      if (invocations.incrementAndGet() == 1) {
        throw new IllegalArgumentException("transient");
      }
      consumed.add(item);
    });

    assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask);

    assertEquals(List.of(2, 3), consumed.stream().sorted().toList());
    assertEquals(3, invocations.get());
    assertTrue(wrapper.isTaskCompleted());
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.getQueueWrapper().isEmpty());
  }

  @Test
  void restartLimitExceededInterruptsUnregistersAndClearsQueue() {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("restart-limit", 5, tasksService);
    wrapper.setMaxConsumerRestarts(1);
    List<Thread> workerThreads = new CopyOnWriteArrayList<>();
    wrapper.addProducer(new ProducerPageDTO<>(), page -> {
      workerThreads.add(Thread.currentThread());
      page.setItems(List.of(1, 2, 3, 4, 5));
      page.setCompleted(true);
      return page;
    });
    wrapper.addConsumer(item -> {
      workerThreads.add(Thread.currentThread());
      throw new IllegalArgumentException("always");
    });

    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask));

    assertEquals("Consumer restart limit exceeded", exception.getMessage());
    assertTrue(wrapper.isInterrupted());
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.getQueueWrapper().isEmpty());
    assertTrue(workerThreads.stream().allMatch(thread -> !thread.isAlive()));
  }

  @Test
  void transientBatchConsumerFailureCreatesReplacementAndDrainsRemainingBatches() {
    TasksService tasksService = new TasksService();
    BatchTaskWrapper<Integer> wrapper = new BatchTaskWrapper<>("recover-batch-consumer", 6, 2, tasksService);
    wrapper.setMaxConsumerRestarts(1);
    AtomicInteger invocations = new AtomicInteger();
    List<Integer> consumed = new CopyOnWriteArrayList<>();
    List<Integer> failedBatch = new CopyOnWriteArrayList<>();
    wrapper.addProducer(new ProducerPageDTO<>(), pagedProducer(List.of(1, 2, 3, 4, 5, 6), 6));
    wrapper.addConsumer((items, dto) -> {
      if (invocations.incrementAndGet() == 1) {
        failedBatch.addAll(items);
        throw new IllegalArgumentException("transient");
      }
      consumed.addAll(new ArrayList<>(items));
    });

    assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask);

    List<Integer> processedItems = new ArrayList<>(failedBatch);
    processedItems.addAll(consumed);
    assertEquals(List.of(1, 2, 3, 4, 5, 6), processedItems.stream().sorted().toList());
    assertTrue(consumed.stream().noneMatch(failedBatch::contains));
    assertTrue(invocations.get() > 1);
    assertTrue(wrapper.isTaskCompleted());
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.getQueueWrapper().isEmpty());
  }

  private static Thread startTask(final TaskWrapper wrapper, final AtomicReference<Throwable> failure) {
    Thread thread = new Thread(() -> {
      try {
        wrapper.executeTask();
      }
      catch (Throwable e) {
        failure.set(e);
      }
    });
    thread.start();
    return thread;
  }

  private static void assertNoFailure(final AtomicReference<Throwable> failure) {
    assertNull(failure.get());
  }

  private static UnaryOperator<ProducerPageDTO<Integer>> pagedProducer(final List<Integer> items, final int pageSize) {
    AtomicInteger offset = new AtomicInteger();
    return page -> {
      int start = offset.getAndAdd(pageSize);
      int end = Math.min(start + pageSize, items.size());
      page.setItems(start >= items.size() ? List.of() : items.subList(start, end));
      page.setCompleted(end >= items.size());
      return page;
    };
  }

  private static UnaryOperator<ProducerPageDTO<Integer>> sequencedProducer(
      final int producerNumber,
      final BasicTaskWrapper<Integer> wrapper,
      final List<Thread> producerThreads,
      final List<Integer> producedItems,
      final AtomicInteger nextProducedItem)
  {
    return page -> {
      producerThreads.add(Thread.currentThread());
      int start = nextProducedItem.getAndAdd(4);
      List<Integer> items = List.of(start, start + 1, start + 2, start + 3);
      producedItems.addAll(items);
      page.setItems(items);
      log.info("producer {} generated items {}; queue size before enqueue {}", producerNumber, items,
          wrapper.getQueueWrapper().size());
      return page;
    };
  }

  private static Thread startQueueMonitor(
      final BasicTaskWrapper<Integer> wrapper,
      final AtomicBoolean monitorRunning)
  {
    Thread thread = new Thread(() -> {
      int previousSize = -1;
      while (monitorRunning.get()) {
        int currentSize = wrapper.getQueueWrapper().size();
        if (currentSize != previousSize) {
          log.info("current queue size {}", currentSize);
          previousSize = currentSize;
        }
        sleep(1L);
      }
      log.info("current queue size {}", wrapper.getQueueWrapper().size());
    }, "queue-size-monitor");
    thread.start();
    return thread;
  }

  private static List<Integer> numberedItems(final int count) {
    List<Integer> items = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      items.add(i);
    }
    return items;
  }

  private static void waitForQueueSize(final BasicTaskWrapper<Integer> wrapper, final int expectedSize)
      throws InterruptedException
  {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
    while (wrapper.getQueueWrapper().size() < expectedSize && System.nanoTime() < deadline) {
      Thread.sleep(1L);
    }
    assertEquals(expectedSize, wrapper.getQueueWrapper().size());
  }

  private static void await(final CountDownLatch latch) {
    try {
      assertTrue(latch.await(2L, TimeUnit.SECONDS));
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
