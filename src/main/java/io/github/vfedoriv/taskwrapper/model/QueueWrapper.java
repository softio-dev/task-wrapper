package io.github.vfedoriv.taskwrapper.model;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Data;

@Data
public class QueueWrapper<T>
{
  private final Integer queueSize;

  private final BlockingQueue<T> blockingQueue;

  private final AtomicBoolean interrupt = new AtomicBoolean(false);

  private final AtomicBoolean producersCompleted = new AtomicBoolean(false);

  private final AtomicBoolean consumersCompleted = new AtomicBoolean(false);

  public QueueWrapper(final Integer queueSize) {
    if (queueSize == null || queueSize <= 0) {
      throw new IllegalArgumentException("Queue size must be greater than zero");
    }
    this.queueSize = queueSize;
    this.blockingQueue = new LinkedBlockingQueue<T>(queueSize);
  }

  public void interrupt() {
      this.interrupt.set(true);
  }

  public void producersComplete() {
      this.producersCompleted.set(true);
  }

  public void consumersComplete() {
      this.consumersCompleted.set(true);
  }

  public boolean isInterrupt() {
    return interrupt.get();
  }

  public boolean isProducersCompleted() {
    return producersCompleted.get();
  }

  public boolean isConsumersCompleted() {
    return consumersCompleted.get();
  }
}
