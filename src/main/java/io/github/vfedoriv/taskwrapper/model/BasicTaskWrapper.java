package io.github.vfedoriv.taskwrapper.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import io.github.vfedoriv.taskwrapper.service.TasksService;
import io.github.vfedoriv.taskwrapper.consumer.BasicConsumer;
import io.github.vfedoriv.taskwrapper.producer.BasicProducer;
import io.github.vfedoriv.taskwrapper.producer.ProducerPageDTO;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicTaskWrapper<T> implements TaskWrapper
{
  private final String taskName;

  private final long threadId;

  private final List<BasicConsumer<T>> consumers = new ArrayList<>();

  private final List<BasicProducer<T>> producers = new ArrayList<>();

  private final QueueWrapper<T> queueWrapper;

  private final TasksService registrarService;

  private ThreadPoolExecutor threadPoolExecutor;

  public BasicTaskWrapper(final int queueSize, final TasksService registrarService) {
    // TODO: with this impl task name will a name of method where task object created
    // so if we call the same method twice, a exception will be thrown when we try to register this task in TaskService
    // consider to create a constructor that accept custom task name instead?
    this.taskName = StackWalker.getInstance()
        .walk(frames -> frames.skip(2).findFirst().map(StackWalker.StackFrame::getMethodName)).orElse("unknown method");
    this.threadId = Thread.currentThread().getId();
    this.queueWrapper = new QueueWrapper<T>(queueSize);
    this.registrarService = registrarService;
  }

  public void addConsumer(final Consumer<T> consumer) {
    try {
      BasicConsumer<T> object = new BasicConsumer<>(this.queueWrapper, consumer);
      consumers.add(object);
    }
    catch (Exception e) {
      log.error("Error on consumer instance creation: {}", e.getMessage());
    }
  }

  public void addConsumers(final Consumer<T> consumer, final int instancesCount) {
    try {
      for (int i= 0; i < instancesCount; i++) {
        BasicConsumer<T> object = new BasicConsumer<>(this.queueWrapper, consumer);
        consumers.add(object);
      }
    }
    catch (Exception e) {
      log.error("Error on consumer instance creation: {}", e.getMessage());
    }
  }

  public void addProducer(final ProducerPageDTO<T> producerPageDTO, final UnaryOperator<ProducerPageDTO<T>> itemsProducerFunction) {
    try {
      BasicProducer<T> producer = new BasicProducer<T>(this.queueWrapper, producerPageDTO, itemsProducerFunction);
      producers.add(producer);
    }
    catch (Exception e) {
      log.error("Error on producer instance creation: {}", e.getMessage());
    }
  }

  @Override
  public void executeTask() {
    register(registrarService);
    try {
      int threadsCount = consumers.size() + producers.size();
      this.threadPoolExecutor = new ThreadPoolExecutor(threadsCount, threadsCount, 0L, TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(threadsCount));
      producers.forEach(task -> threadPoolExecutor.submit(task));
      consumers.forEach(task -> threadPoolExecutor.submit(task));
      while (!isTaskCompleted()) {
        log.trace("wait until all internal tasks completed");
        try {
          TimeUnit.MILLISECONDS.sleep(1);
        }
        catch (InterruptedException e) {
          throw new RuntimeException("Interrupted: ", e);
        }
      }
      queueWrapper.interrupt();
    } finally {
      unregister(registrarService);
    }
  }

  @Override
  public void interrupt() {
    queueWrapper.interrupt();
  }

  @Override
  public boolean isInterrupted() {
    return queueWrapper.isInterrupt();
  }

  @Override
  public boolean isProducersCompleted() {
    for (BasicProducer<T> producer : producers) {
      if (!producer.isCompleted()) {
        return false;
      }
    }
    queueWrapper.producersComplete();
    log.trace("producers completed");
    return true;
  }

  @Override
  public boolean isConsumersCompleted() {
    for (BasicConsumer<T> consumer : consumers) {
      if (!consumer.isCompleted()) {
        return false;
      }
    }
    queueWrapper.consumersComplete();
    log.trace("consumers completed");
    return true;
  }

  @Override
  public boolean isTaskCompleted() {
    return isProducersCompleted() && isConsumersCompleted();
  }

  public List<BasicConsumer<T>> getConsumers() {
    return consumers;
  }

  public List<BasicProducer<T>> getProducers() {
    return producers;
  }

  @Override
  public String getTaskName() {
    return taskName;
  }

  @Override
  public void register(TasksService tasksService) {
    if (tasksService != null) {
      tasksService.addTask(this);
    }
  }

  @Override
  public void unregister(TasksService tasksService) {
    if (tasksService != null) {
      tasksService.removeTask(this);
    }
  }
}
