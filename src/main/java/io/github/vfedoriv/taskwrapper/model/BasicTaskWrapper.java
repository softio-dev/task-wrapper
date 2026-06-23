package io.github.vfedoriv.taskwrapper.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
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

  private final List<BasicConsumer<T>> consumers = new ArrayList<>();

  private final List<BasicProducer<T>> producers = new ArrayList<>();

  private final QueueWrapper<T> queueWrapper;

  private final TasksService registrarService;

  public BasicTaskWrapper(final int queueSize, final TasksService registrarService) {
    this("task-" + UUID.randomUUID(), queueSize, registrarService);
  }

  public BasicTaskWrapper(final String taskName, final int queueSize, final TasksService registrarService) {
    this.taskName = Objects.requireNonNull(taskName, "Task name is required");
    this.queueWrapper = new QueueWrapper<T>(queueSize);
    this.registrarService = registrarService;
  }

  public void addConsumer(final Consumer<T> consumer) {
    BasicConsumer<T> object = new BasicConsumer<>(this.queueWrapper, consumer);
    consumers.add(object);
  }

  public void addConsumers(final Consumer<T> consumer, final int instancesCount) {
    if (instancesCount <= 0) {
      throw new IllegalArgumentException("Instances count must be greater than zero");
    }
    for (int i= 0; i < instancesCount; i++) {
      addConsumer(consumer);
    }
  }

  public void addProducer(final ProducerPageDTO<T> producerPageDTO, final UnaryOperator<ProducerPageDTO<T>> itemsProducerFunction) {
    BasicProducer<T> producer = new BasicProducer<T>(this.queueWrapper, producerPageDTO, itemsProducerFunction);
    producers.add(producer);
  }

  @Override
  public void executeTask() {
    register(registrarService);
    try {
      TaskLifecycleRunner.run(producers, consumers, queueWrapper);
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
    return producers.stream().allMatch(BasicProducer::isCompleted);
  }

  @Override
  public boolean isConsumersCompleted() {
    return consumers.stream().allMatch(BasicConsumer::isCompleted);
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
