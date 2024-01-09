package io.github.vfedoriv.taskwrapper.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchConsumer<T>
    implements Runnable
{
  protected final QueueWrapper<T> queueWrapper;

  private final BiConsumer<Collection<T>, Object> biConsumer;

  private boolean completed = false;

  private final int batchSize;

  private final Object consumerDTO;

  public BatchConsumer(
      final QueueWrapper<T> queueWrapper,
      final BiConsumer<Collection<T>, Object> biConsumer,
      final Object consumerDTO,
      final int batchSize)
  {
    this.biConsumer = biConsumer;
    this.queueWrapper = queueWrapper;
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
    while (!Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt()) {
      try {
        if (queueWrapper.isProducersCompleted() && getQueue().isEmpty()) {
          setCompleted(true);
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
      catch (Throwable e) {
        log.error("Exception {} in thread [{}]. Will be interrupted. Stack trace: {}", Thread.currentThread().getName(),
            e.getMessage(), e.getStackTrace());
        Thread.currentThread().interrupt();
      }
    }
    setCompleted(true);
    log.debug("Consumer thread [{}] . Done", Thread.currentThread().getName());
  }

  public boolean isCompleted() {
    return completed;
  }

  public void setCompleted(final boolean completed) {
    this.completed = completed;
  }
}
