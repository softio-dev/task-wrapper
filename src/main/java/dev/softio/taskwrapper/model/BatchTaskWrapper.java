package dev.softio.taskwrapper.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import dev.softio.taskwrapper.service.TasksService;
import dev.softio.taskwrapper.consumer.BatchConsumer;
import dev.softio.taskwrapper.producer.BasicProducer;
import dev.softio.taskwrapper.producer.ProducerPageDTO;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchTaskWrapper<T>
    implements TaskWrapper
{
  private final String taskName;

  private final List<BatchConsumer<T>> consumers = new ArrayList<>();

  private final List<BasicProducer<T>> producers = new ArrayList<>();

  private final QueueWrapper<T> queueWrapper;

  private final TasksService registrarService;

  private final int batchSize;

  private int maxConsumerRestarts = 10;

  public BatchTaskWrapper(final int queueSize, final int batchSize, final TasksService registrarService) {
    this("task-" + UUID.randomUUID(), queueSize, batchSize, registrarService);
  }

  public BatchTaskWrapper(final String taskName, final int queueSize, final int batchSize, final TasksService registrarService) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Batch size must be greater than zero");
    }
    this.taskName = Objects.requireNonNull(taskName, "Task name is required");
    this.queueWrapper = new QueueWrapper<T>(queueSize);
    this.registrarService = registrarService;
    this.batchSize = batchSize;
  }

  public void addConsumer(final BiConsumer<Collection<T>, Object> biConsumer, final Object consumerDTO) {
    BatchConsumer<T> object = new BatchConsumer<>(this.queueWrapper, biConsumer, consumerDTO, batchSize);
    consumers.add(object);
  }

  public void addConsumer(final BiConsumer<Collection<T>, Object> biConsumer) {
    addConsumer(biConsumer, null);
  }

  public void addConsumers(final BiConsumer<Collection<T>, Object> biConsumer, final Object consumerDTO, final int instancesCount) {
    if (instancesCount <= 0) {
      throw new IllegalArgumentException("Instances count must be greater than zero");
    }
    for (int i = 0; i < instancesCount; i++) {
      addConsumer(biConsumer, consumerDTO);
    }
  }

  public void addConsumers(final BiConsumer<Collection<T>, Object> biConsumer, final int instancesCount) {
    addConsumers(biConsumer, null, instancesCount);
  }

  public void addProducer(final ProducerPageDTO<T> producerPageDTO, final UnaryOperator<ProducerPageDTO<T>> itemsProducerFunction) {
    BasicProducer<T> producer = new BasicProducer<T>(this.queueWrapper, producerPageDTO, itemsProducerFunction);
    producers.add(producer);
  }

  public void setMaxConsumerRestarts(final int maxConsumerRestarts) {
    if (maxConsumerRestarts < 0) {
      throw new IllegalArgumentException("Max consumer restarts must not be negative");
    }
    this.maxConsumerRestarts = maxConsumerRestarts;
  }

  @Override
  public void executeTask() {
    register(registrarService);
    try {
      TaskLifecycleRunner.run(producers, consumers, queueWrapper, maxConsumerRestarts);
    }
    finally {
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
    return consumers.stream().allMatch(BatchConsumer::isCompleted);
  }

  @Override
  public boolean isTaskCompleted() {
    return isProducersCompleted() && isConsumersCompleted();
  }

  public List<BatchConsumer<T>> getConsumers() {
    return consumers;
  }

  public List<BasicProducer<T>> getProducers() {
    return producers;
  }

  public QueueWrapper<T> getQueueWrapper() {
    return queueWrapper;
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
