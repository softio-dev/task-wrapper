package io.github.vfedoriv.taskwrapper.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import org.junit.jupiter.api.Test;

class ConsumerTests
{
  @Test
  void basicConsumerDrainsQueueAfterProducersComplete() {
    QueueWrapper<Integer> queueWrapper = new QueueWrapper<>(10);
    queueWrapper.getBlockingQueue().addAll(List.of(1, 2, 3));
    queueWrapper.producersComplete();
    List<Integer> consumed = new CopyOnWriteArrayList<>();
    BasicConsumer<Integer> consumer = new BasicConsumer<>(queueWrapper, consumed::add);

    assertTimeoutPreemptively(Duration.ofSeconds(1), consumer::run);

    assertEquals(List.of(1, 2, 3), consumed);
    assertTrue(consumer.isCompleted());
  }

  @Test
  void batchConsumerProcessesFullAndPartialBatches() {
    QueueWrapper<Integer> queueWrapper = new QueueWrapper<>(10);
    queueWrapper.getBlockingQueue().addAll(List.of(1, 2, 3, 4, 5));
    queueWrapper.producersComplete();
    List<List<Integer>> batches = new CopyOnWriteArrayList<>();
    BatchConsumer<Integer> consumer = new BatchConsumer<>(queueWrapper,
        (items, dto) -> batches.add(new ArrayList<>(items)), null, 3);

    assertTimeoutPreemptively(Duration.ofSeconds(1), consumer::run);

    assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5)), batches);
    assertTrue(consumer.isCompleted());
  }

  @Test
  void basicConsumerPropagatesConsumerException() {
    QueueWrapper<Integer> queueWrapper = new QueueWrapper<>(1);
    queueWrapper.getBlockingQueue().add(1);
    queueWrapper.producersComplete();
    BasicConsumer<Integer> consumer = new BasicConsumer<>(queueWrapper, item -> {
      throw new IllegalArgumentException("boom");
    });

    assertThrows(IllegalArgumentException.class, consumer::run);
    assertTrue(consumer.isCompleted());
  }

  @Test
  void batchConsumerStopsWhenInterrupted() {
    QueueWrapper<Integer> queueWrapper = new QueueWrapper<>(1);
    BatchConsumer<Integer> consumer = new BatchConsumer<>(queueWrapper, (Collection<Integer> items, Object dto) -> {
    }, null, 2);

    queueWrapper.interrupt();

    assertTimeoutPreemptively(Duration.ofSeconds(1), consumer::run);
    assertTrue(consumer.isCompleted());
  }
}
