package io.github.vfedoriv.taskwrapper.consumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.Objects;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicConsumer<T>
    implements Runnable
{
  protected final QueueWrapper<T> queueWrapper;

  private final Consumer<T> consumer;

  private final AtomicBoolean completed = new AtomicBoolean(false);

  public BasicConsumer(final QueueWrapper<T> queueWrapper, final Consumer<T> consumer) {
    this.consumer = Objects.requireNonNull(consumer, "Consumer function is required");
    this.queueWrapper = Objects.requireNonNull(queueWrapper, "Queue wrapper is required");
  }

  public BasicConsumer<T> newReplacement() {
    return new BasicConsumer<>(queueWrapper, consumer);
  }

  @Override
  public void run() {
    consume();
  }

  protected BlockingQueue<T> getQueue() {
    return this.queueWrapper.getBlockingQueue();
  }

  protected void consume() {
    try {
      while (!Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt()) {
        if (queueWrapper.isProducersCompleted() && getQueue().isEmpty()) {
          break;
        }
        log.trace("Consuming item from queue");
        T item = getQueue().poll(10, TimeUnit.MILLISECONDS);
        if (item == null) {
          log.trace("No items in queue");
          continue;
        }
        log.trace("Consumer thread [{}] consumed one item from queue", Thread.currentThread().getName());
        // process item
        this.consumer.accept(item);
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Consumer was interrupted", e);
    }
    finally {
      setCompleted(true);
      log.debug("Consumer thread [{}] . Done", Thread.currentThread().getName());
    }
    if (Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt()) {
      throw new IllegalStateException("Consumer was interrupted");
    }
  }

  public boolean isCompleted() {
    return this.completed.get();
  }

  public void setCompleted(final boolean completed) {
    this.completed.set(completed);
  }
}
