package io.github.vfedoriv.taskwrapper.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchConsumer<T>
    implements Runnable
{
  protected final QueueWrapper<T> queueWrapper;

  private final BiConsumer<Collection<T>, Object> biConsumer;

  private final AtomicBoolean completed = new AtomicBoolean(false);

  private final int batchSize;

  private final Object consumerDTO;

  public BatchConsumer(
      final QueueWrapper<T> queueWrapper,
      final BiConsumer<Collection<T>, Object> biConsumer,
      final Object consumerDTO,
      final int batchSize)
  {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Batch size must be greater than zero");
    }
    this.biConsumer = Objects.requireNonNull(biConsumer, "Batch consumer function is required");
    this.queueWrapper = Objects.requireNonNull(queueWrapper, "Queue wrapper is required");
    this.batchSize = batchSize;
    this.consumerDTO = consumerDTO;
  }

  @Override
  public void run() {
    consume();
  }

  protected BlockingQueue<T> getQueue() {
    return this.queueWrapper.getBlockingQueue();
  }

  protected void consume() {
    Collection<T> items = new ArrayList<>();
    try {
      while (!Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt()) {
        if (queueWrapper.isProducersCompleted() && getQueue().isEmpty()) {
          break;
        }
        log.trace("Consuming items from queue");
        items.clear();
        getQueue().drainTo(items, batchSize);
        if (items.isEmpty()) {
          log.trace("No items in queue");
          TimeUnit.MILLISECONDS.sleep(1L);
          continue;
        }
        log.trace("Consumer thread [{}] consumed {} items from queue", Thread.currentThread().getName(), items.size());
        // process items
        this.biConsumer.accept(items, consumerDTO);
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Batch consumer was interrupted", e);
    }
    finally {
      setCompleted(true);
      log.debug("Consumer thread [{}] . Done", Thread.currentThread().getName());
    }
    if (Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt()) {
      throw new IllegalStateException("Batch consumer was interrupted");
    }
  }

  public boolean isCompleted() {
    return completed.get();
  }

  public void setCompleted(final boolean completed) {
    this.completed.set(completed);
  }
}
