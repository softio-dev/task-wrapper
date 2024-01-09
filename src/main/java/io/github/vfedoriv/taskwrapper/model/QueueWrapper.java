package io.github.vfedoriv.taskwrapper.model;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import lombok.Data;

@Data
public class QueueWrapper<T>
{
  private final Integer queueSize;

  private final BlockingQueue<T> blockingQueue;

  private boolean interrupt = false;

  private boolean producersCompleted = false;

  private boolean consumersCompleted = false;

  public QueueWrapper(final Integer queueSize) {
    this.queueSize = queueSize;
    this.blockingQueue = new LinkedBlockingQueue<T>(queueSize);
  }

  public void interrupt() {
      this.interrupt = true;
  }

  public void producersComplete() {
      this.producersCompleted = true;
  }

  public void consumersComplete() {
      this.consumersCompleted = true;
  }
}
