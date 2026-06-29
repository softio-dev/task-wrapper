package dev.softio.taskwrapper.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import dev.softio.taskwrapper.producer.ProducerPageDTO;
import dev.softio.taskwrapper.service.TasksService;

import org.junit.jupiter.api.Test;

class TaskWrapperExecutionTests
{
  @Test
  void basicWrapperExecutesAllItemsAndUnregisters() {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("basic", 2, tasksService);
    List<Integer> consumed = new CopyOnWriteArrayList<>();
    wrapper.addProducer(new ProducerPageDTO<>(), pagedProducer(List.of(1, 2, 3, 4, 5), 2));
    wrapper.addConsumers(consumed::add, 2);

    assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask);

    assertEquals(List.of(1, 2, 3, 4, 5), consumed.stream().sorted().toList());
    assertTrue(wrapper.isTaskCompleted());
    assertFalse(tasksService.hasTask(wrapper));
  }

  @Test
  void batchWrapperProcessesFullAndPartialBatches() {
    TasksService tasksService = new TasksService();
    BatchTaskWrapper<Integer> wrapper = new BatchTaskWrapper<>("batch", 2, 3, tasksService);
    List<List<Integer>> batches = new CopyOnWriteArrayList<>();
    wrapper.addProducer(new ProducerPageDTO<>(), pagedProducer(List.of(1, 2, 3, 4, 5), 5));
    wrapper.addConsumer((items, dto) -> batches.add(new ArrayList<>(items)));

    assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask);

    assertEquals(List.of(1, 2, 3, 4, 5), batches.stream().flatMap(Collection::stream).sorted().toList());
    assertTrue(batches.stream().allMatch(batch -> batch.size() <= 3));
    assertFalse(tasksService.hasTask(wrapper));
  }

  @Test
  void producerFailureFailsWholeTaskAndUnregisters() {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("producer-failure", 2, tasksService);
    wrapper.addProducer(new ProducerPageDTO<>(), page -> {
      throw new IllegalArgumentException("boom");
    });
    wrapper.addConsumer(item -> {
    });

    assertThrows(IllegalStateException.class,
        () -> assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask));
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.isInterrupted());
  }

  @Test
  void consumerFailureFailsWholeTaskAndUnregisters() {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("consumer-failure", 1, tasksService);
    wrapper.setMaxConsumerRestarts(0);
    wrapper.addProducer(new ProducerPageDTO<>(), pagedProducer(List.of(1, 2, 3), 3));
    wrapper.addConsumer(item -> {
      throw new IllegalArgumentException("boom");
    });

    assertThrows(IllegalStateException.class,
        () -> assertTimeoutPreemptively(Duration.ofSeconds(2), wrapper::executeTask));
    assertFalse(tasksService.hasTask(wrapper));
    assertTrue(wrapper.isInterrupted());
  }

  @Test
  void wrapperCanBeInterruptedExplicitly() throws InterruptedException {
    TasksService tasksService = new TasksService();
    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("interrupt", 1, tasksService);
    wrapper.addProducer(new ProducerPageDTO<>(), repeatingProducer());
    wrapper.addConsumer(item -> sleep(25));

    Thread thread = new Thread(wrapper::executeTask);
    thread.start();
    while (!tasksService.hasTask(wrapper)) {
      Thread.sleep(1);
    }

    wrapper.interrupt();
    thread.join(2_000L);

    assertFalse(thread.isAlive());
    assertTrue(wrapper.isInterrupted());
    assertFalse(tasksService.hasTask(wrapper));
  }

  @Test
  void validatesConfigurationBeforeExecution() {
    assertThrows(IllegalArgumentException.class, () -> new BasicTaskWrapper<Integer>("bad", 0, null));
    assertThrows(IllegalArgumentException.class, () -> new BatchTaskWrapper<Integer>("bad", 1, 0, null));

    BasicTaskWrapper<Integer> wrapper = new BasicTaskWrapper<>("empty", 1, null);
    assertThrows(IllegalStateException.class, wrapper::executeTask);
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

  private static UnaryOperator<ProducerPageDTO<Integer>> repeatingProducer() {
    return page -> {
      page.setItems(List.of(1));
      return page;
    };
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
