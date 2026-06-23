package io.github.vfedoriv.taskwrapper.producer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicProducer<T>
    implements Runnable
{
  private final QueueWrapper<T> queueWrapper;

  private final UnaryOperator<ProducerPageDTO<T>> itemsProducerFunction;

  private final AtomicBoolean completed = new AtomicBoolean(false);

  private ProducerPageDTO<T> pageDTO;

  public BasicProducer(
      final QueueWrapper<T> queueWrapper,
      final ProducerPageDTO<T> producerPageDTO,
      final UnaryOperator<ProducerPageDTO<T>> itemsProducerFunction)
  {
    this.queueWrapper = Objects.requireNonNull(queueWrapper, "Queue wrapper is required");
    this.itemsProducerFunction = Objects.requireNonNull(itemsProducerFunction, "Producer function is required");
    this.pageDTO = Objects.requireNonNull(producerPageDTO, "Producer page DTO is required");
  }

  public boolean isCompleted() {
    return completed.get();
  }

  public void setCompleted(final boolean completed) {
    this.completed.set(completed);
  }

  @Override
  public void run() {
    produce();
  }

  protected BlockingQueue<T> getQueue() {
    return this.queueWrapper.getBlockingQueue();
  }

  protected void produce() {
    try {
      while (!Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt() && !this.pageDTO.isCompleted()) {
        pageDTO = Objects.requireNonNull(itemsProducerFunction.apply(pageDTO), "Producer function returned null");
        List<T> items = Objects.requireNonNull(pageDTO.getItems(), "Producer page items must not be null");
        log.debug("Items produced: {}", items.size());
        if (items.isEmpty() || queueWrapper.isInterrupt()) {
          break;
        }
        for (T item : items) {
          if (queueWrapper.isInterrupt()) {
            break;
          }
          log.trace("put item {} in queue - start", item);
          while (!queueWrapper.isInterrupt() && !getQueue().offer(item, 10, TimeUnit.MILLISECONDS)) {
            log.trace("Queue is full; waiting to put item {}", item);
          }
          log.info("Producer thread [{}] enqueued item {}; current queue size {}", Thread.currentThread().getName(), item,
              getQueue().size());
          log.trace("put item {} in queue - done", item);
        }
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Producer was interrupted", e);
    }
    finally {
      setCompleted(true);
      log.debug("Producer thread [{}] . Done", Thread.currentThread().getName());
    }
    if (Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt()) {
      throw new IllegalStateException("Producer was interrupted");
    }
  }

  public ProducerPageDTO<T> getPageDTO() {
    return pageDTO;
  }
}
