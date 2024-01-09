package io.github.vfedoriv.taskwrapper.consumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicConsumer<T>
    implements Runnable
{
  protected final QueueWrapper<T> queueWrapper;

  private final Consumer<T> consumer;

  private boolean completed = false;

  public BasicConsumer(final QueueWrapper<T> queueWrapper, final Consumer<T> consumer) {
    this.consumer = consumer;
    this.queueWrapper = queueWrapper;
  }

  @Override
  public void run() {
    consume();
  }

  protected BlockingQueue<T> getQueue() {
    return this.queueWrapper.getBlockingQueue();
  }

  protected void consume() {
    while (!Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt()) {
      try {
        if (queueWrapper.isProducersCompleted() && getQueue().isEmpty()) {
          setCompleted(true);
          break;
        }
        log.trace("Consuming item from queue");
        T item = getQueue().poll(1, TimeUnit.MILLISECONDS);
        if (item == null) {
          log.trace("No items in queue");
          continue;
        }
        log.trace("Consumer thread [{}] consumed one item from queue", Thread.currentThread().getName());
        // process item
        this.consumer.accept(item);
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
    return this.completed;
  }

  public void setCompleted(final boolean completed) {
    this.completed = completed;
  }
}
