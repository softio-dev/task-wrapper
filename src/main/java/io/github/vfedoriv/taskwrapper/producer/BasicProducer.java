package io.github.vfedoriv.taskwrapper.producer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import io.github.vfedoriv.taskwrapper.model.QueueWrapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicProducer<T>
    implements Runnable
{
  private final QueueWrapper<T> queueWrapper;

  private final UnaryOperator<ProducerPageDTO<T>> itemsProducerFunction;

  private boolean completed = false;

  private ProducerPageDTO<T> pageDTO;

  public BasicProducer(
      final QueueWrapper<T> queueWrapper,
      final ProducerPageDTO<T> producerPageDTO,
      final UnaryOperator<ProducerPageDTO<T>> itemsProducerFunction)
  {
    this.queueWrapper = queueWrapper;
    this.itemsProducerFunction = itemsProducerFunction;
    this.pageDTO = producerPageDTO;
  }

  public boolean isCompleted() {
    if (queueWrapper.isInterrupt()) {
      completed = true;
      log.info("Producer thread [{}] . Interrupted", Thread.currentThread().getName());
      Thread.currentThread().interrupt();
    }
    return completed;
  }

  public void setCompleted(final boolean completed) {
    this.completed = completed;
  }

  @Override
  public void run() {
    produce();
  }

  protected BlockingQueue<T> getQueue() {
    return this.queueWrapper.getBlockingQueue();
  }

  protected void produce() {
    while (!Thread.currentThread().isInterrupted() && !queueWrapper.isInterrupt() && !this.pageDTO.isCompleted() &&
        !isCompleted()) {
      try {
        pageDTO = itemsProducerFunction.apply(pageDTO);
        List<T> items = pageDTO.getItems();
        log.debug("Items produced: {}", items.size());
        if (items.isEmpty() || queueWrapper.isInterrupt()) {
          setCompleted(true);
          break;
        }
        for (T item : items) {
          log.trace("put item {} in queue - start", item);
          // the thread will wait here if queue is full
          getQueue().put(item);
          log.trace("put item {} in queue - done", item);
        }
      }
      catch (Throwable e) {
        log.error("Exception {} in thread [{}]. Will be interrupted. Stack trace: {}", Thread.currentThread().getName(),
            e.getMessage(), e.getStackTrace());
        Thread.currentThread().interrupt();
      }
    }
    setCompleted(true);
    log.debug("Producer thread [{}] . Done", Thread.currentThread().getName());
  }

  public ProducerPageDTO<T> getPageDTO() {
    return pageDTO;
  }
}
