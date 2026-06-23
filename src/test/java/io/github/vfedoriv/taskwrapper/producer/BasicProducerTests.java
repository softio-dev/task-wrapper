package io.github.vfedoriv.taskwrapper.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import org.junit.jupiter.api.Test;

class BasicProducerTests
{
  @Test
  void enqueuesAllProducedItemsUntilEmptyPage() {
    QueueWrapper<Integer> queueWrapper = new QueueWrapper<>(10);
    AtomicInteger calls = new AtomicInteger();
    ProducerPageDTO<Integer> page = new ProducerPageDTO<>();
    BasicProducer<Integer> producer = new BasicProducer<>(queueWrapper, page, current -> {
      if (calls.incrementAndGet() == 1) {
        current.setItems(List.of(1, 2, 3));
      }
      else {
        current.setItems(List.of());
      }
      return current;
    });

    assertTimeoutPreemptively(Duration.ofSeconds(1), producer::run);

    assertEquals(List.of(1, 2, 3), queueWrapper.getBlockingQueue().stream().toList());
    assertTrue(producer.isCompleted());
  }

  @Test
  void stopsOnCompletedPage() {
    QueueWrapper<Integer> queueWrapper = new QueueWrapper<>(10);
    ProducerPageDTO<Integer> page = new ProducerPageDTO<>();
    page.setCompleted(true);
    BasicProducer<Integer> producer = new BasicProducer<>(queueWrapper, page, current -> {
      throw new AssertionError("Should not be called");
    });

    assertTimeoutPreemptively(Duration.ofSeconds(1), producer::run);

    assertTrue(queueWrapper.getBlockingQueue().isEmpty());
    assertTrue(producer.isCompleted());
  }

  @Test
  void propagatesProducerExceptions() {
    BasicProducer<Integer> producer = new BasicProducer<>(new QueueWrapper<>(1), new ProducerPageDTO<>(), page -> {
      throw new IllegalArgumentException("boom");
    });

    assertThrows(IllegalArgumentException.class, producer::run);
    assertTrue(producer.isCompleted());
  }
}
