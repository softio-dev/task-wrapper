package dev.softio.taskwrapper.examples;

import dev.softio.taskwrapper.model.BasicTaskWrapper;
import dev.softio.taskwrapper.model.TaskWrapper;
import dev.softio.taskwrapper.producer.ProducerPageDTO;
import dev.softio.taskwrapper.service.TasksService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RepositoryTaskWrapperExample
{
  private static final String TASK_NAME = "example-repository-range-task";

  private static final String STARTING_ITEM_ID = "item-000";

  private static final int QUEUE_SIZE = 100;

  private static final int PAGE_SIZE = 2;

  private final RepositoryProducerHelper repositoryProducerHelper;

  private final TasksService tasksService;

  public RepositoryTaskWrapperExample(
      final RepositoryProducerHelper repositoryProducerHelper,
      final TasksService tasksService)
  {
    this.repositoryProducerHelper = repositoryProducerHelper;
    this.tasksService = tasksService;
  }

  public TaskWrapper createTask() {
    BasicTaskWrapper<ExampleItem> taskWrapper = new BasicTaskWrapper<>(TASK_NAME, QUEUE_SIZE, tasksService);
    taskWrapper.addProducer(
        new ProducerPageDTO<>(STARTING_ITEM_ID, PAGE_SIZE),
        repositoryProducerHelper::produceExampleItems);
    taskWrapper.addConsumer(this::processItem);
    return taskWrapper;
  }

  public void runTask() {
    createTask().executeTask();
  }

  private void processItem(final ExampleItem item) {
    log.info("Processed example item {}", item);
  }
}
