# task-wrapper

`task-wrapper` is a small Spring Boot library for running producer/consumer tasks over a bounded in-memory queue.

The main API is:

- `TasksService`: registry for currently running tasks. Use it to list, interrupt, and clean up tasks by name. The class name is plural in this codebase.
- `TaskWrapper`: common task contract implemented by the concrete wrappers.
- `BasicTaskWrapper<T>`: runs one or more producers and item-by-item consumers.
- `BatchTaskWrapper<T>`: runs one or more producers and batch consumers.
- `ProducerPageDTO<T>`: mutable paging state passed through producer functions.

## Requirements

- Java 25
- Maven Wrapper from this repository
- Spring Boot 4.1.0

## Build and Test

```bash
./mvnw test
./mvnw package
./mvnw spring-boot:run
```

## Core Concepts

A task is built from:

1. A bounded queue size.
2. At least one producer.
3. At least one consumer.
4. An optional task name.
5. A `TasksService` instance for runtime registration.

`executeTask()` is blocking. It starts all producers and consumers, waits for them to finish, interrupts worker threads during cleanup, clears the queue, and unregisters the task from `TasksService`.

Because `executeTask()` blocks, run it on a background thread when starting tasks from a web request, scheduler, or other interactive flow.

## Producer Page State

`ProducerPageDTO<T>` is mutable state owned by one producer. Create a separate instance for each producer and do not share it across producer threads.

The DTO is not thread-safe: its fields are plain mutable values, `counter` updates are not atomic, and `items` may be a mutable list. Do not read or mutate a producer's `ProducerPageDTO` from another thread while the task is running. To stop a running task from another thread, use `TaskWrapper.interrupt()` or `TasksService.interrupt(...)` instead of calling `page.setCompleted(true)`.

Field purposes:

- `id`: producer-defined identifier or cursor. In ranged repository queries, this can hold the starting id or key.
- `pageSize`: producer-defined fetch size or page size.
- `counter`: producer-defined iteration counter. Increment it from the producer function when offset-based paging needs it.
- `iteratedValue`: optional producer-defined progress value, such as a processed item count.
- `lastItem`: optional producer-defined cursor state. The repository example uses it to remember the last item returned by the previous page.
- `items`: the items produced by the current producer call. It must not be `null`; use `List.of()` for no items.
- `completed`: set to `true` when the producer should not be called again.

The producer loop stops when `completed` is `true`, when the producer returns an empty `items` list, or when the task is interrupted.

## Basic Item-by-Item Task

Use `BasicTaskWrapper<T>` when each consumer invocation should receive one item.

```java
package example;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.github.vfedoriv.taskwrapper.model.BasicTaskWrapper;
import io.github.vfedoriv.taskwrapper.producer.ProducerPageDTO;
import io.github.vfedoriv.taskwrapper.service.TasksService;

import org.springframework.stereotype.Service;

@Service
public class UserExportService
{
  private final TasksService tasksService;

  public UserExportService(final TasksService tasksService) {
    this.tasksService = tasksService;
  }

  public String startExport(final List<String> userIds) {
    BasicTaskWrapper<String> task = new BasicTaskWrapper<>(
        "user-export",
        100,
        tasksService);

    task.addProducer(new ProducerPageDTO<>("users", 25), page -> {
      int pageSize = page.getPageSize();
      int start = page.getCounter() * pageSize;
      int end = Math.min(start + pageSize, userIds.size());

      page.setItems(start >= userIds.size() ? List.of() : userIds.subList(start, end));
      page.setCompleted(end >= userIds.size());
      page.incrementCounter();
      return page;
    });

    task.addConsumers(this::exportOneUser, 4);

    CompletableFuture.runAsync(task::executeTask);
    return task.getTaskName();
  }

  private void exportOneUser(final String userId) {
    // Write one user to a file, call another service, etc.
  }
}
```

What happens:

- The producer function is called repeatedly until `page.isCompleted()` is `true`, it returns an empty item list, or the task is interrupted.
- Produced items are put into the bounded queue.
- Four consumer instances drain the queue concurrently.
- The task unregisters itself from `TasksService` when it finishes or fails.

## Repository Ranged Query Producer

Use `ProducerPageDTO<T>` to keep the paging cursor for repository queries that load items by range, for example `id > lastSeenId limit pageSize`.

The repository should return a stable ordered page after the last processed item:

```java
@Component
public class ExampleRepository
{
  private final List<ExampleItem> items = List.of(
      new ExampleItem("item-001", "First example item"),
      new ExampleItem("item-002", "Second example item"),
      new ExampleItem("item-003", "Third example item"));

  public List<ExampleItem> findExampleItemsByRange(
      final String lastSeenId,
      final Integer pageSize)
  {
    return items.stream()
        .filter(item -> item.getId().compareTo(lastSeenId) > 0)
        .limit(pageSize)
        .toList();
  }
}
```

`RepositoryProducerHelper` adapts that repository method to the producer function signature expected by `BasicTaskWrapper`:

```java
@Component
public class RepositoryProducerHelper
{
  private final ExampleRepository exampleRepository;

  public RepositoryProducerHelper(final ExampleRepository exampleRepository) {
    this.exampleRepository = exampleRepository;
  }

  public ProducerPageDTO<ExampleItem> produceExampleItems(
      final ProducerPageDTO<ExampleItem> pageDTO)
  {
    return produceItems(pageDTO, exampleRepository::findExampleItemsByRange);
  }
}
```

Then wire the helper into a task:

```java
@Component
public class RepositoryTaskWrapperExample
{
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
    BasicTaskWrapper<ExampleItem> taskWrapper = new BasicTaskWrapper<>(
        "example-repository-range-task",
        100,
        tasksService);

    taskWrapper.addProducer(
        new ProducerPageDTO<>("item-000", 2),
        repositoryProducerHelper::produceExampleItems);

    taskWrapper.addConsumer(this::processItem);
    return taskWrapper;
  }

  public void runTask() {
    createTask().executeTask();
  }

  private void processItem(final ExampleItem item) {
    // Process one item loaded from the repository range.
  }
}
```

In this pattern:

- `id` stores the initial cursor value, such as `item-000`.
- `pageSize` becomes the repository query limit.
- `lastItem` stores the last item returned by the previous repository page.
- The next producer call uses `lastItem.getId()` as the next cursor.
- An empty repository page marks the producer as complete.

The complete sample lives under `src/main/java/io/github/vfedoriv/taskwrapper/examples`.

## Batch Task

Use `BatchTaskWrapper<T>` when each consumer invocation should receive up to `batchSize` items.

```java
package example;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.github.vfedoriv.taskwrapper.model.BatchTaskWrapper;
import io.github.vfedoriv.taskwrapper.producer.ProducerPageDTO;
import io.github.vfedoriv.taskwrapper.service.TasksService;

import org.springframework.stereotype.Service;

@Service
public class BatchImportService
{
  private final TasksService tasksService;

  public BatchImportService(final TasksService tasksService) {
    this.tasksService = tasksService;
  }

  public String startImport(final List<String> records) {
    BatchTaskWrapper<String> task = new BatchTaskWrapper<>(
        "record-import",
        500,
        50,
        tasksService);

    task.addProducer(new ProducerPageDTO<>("records", 100), page -> {
      int pageSize = page.getPageSize();
      int start = page.getCounter() * pageSize;
      int end = Math.min(start + pageSize, records.size());

      page.setItems(start >= records.size() ? List.of() : records.subList(start, end));
      page.setCompleted(end >= records.size());
      page.incrementCounter();
      return page;
    });

    task.addConsumers(this::saveBatch, 2);

    CompletableFuture.runAsync(task::executeTask);
    return task.getTaskName();
  }

  private void saveBatch(final Collection<String> items, final Object context) {
    // Persist the batch. The collection size is at most the wrapper batch size.
  }
}
```

`BatchTaskWrapper` also supports passing a consumer context object:

```java
ImportContext context = new ImportContext("tenant-a");

task.addConsumers((items, dto) -> {
  ImportContext importContext = (ImportContext) dto;
  saveForTenant(importContext.tenantId(), items);
}, context, 2);
```

## Using `TasksService`

`TasksService` stores currently registered tasks by task name. A wrapper registers itself when `executeTask()` starts and unregisters itself in a `finally` block when execution ends.

Only one active task can use a given name. Registering another task with the same name throws `IllegalStateException`.

```java
import java.util.Collection;

import io.github.vfedoriv.taskwrapper.model.TaskWrapper;
import io.github.vfedoriv.taskwrapper.service.TasksService;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TaskController
{
  private final TasksService tasksService;

  public TaskController(final TasksService tasksService) {
    this.tasksService = tasksService;
  }

  @GetMapping("/tasks")
  public Collection<TaskWrapper> tasks() {
    return tasksService.getTasks();
  }

  @DeleteMapping("/tasks/{taskName}")
  public void interrupt(@PathVariable final String taskName) {
    tasksService.interrupt(taskName);
  }
}
```

Useful operations:

```java
tasksService.getTasks();                // Currently registered tasks
tasksService.hasTask(task);             // True when this exact task object is registered
tasksService.interrupt("user-export");  // Interrupt a running task by name
tasksService.isInterrupted("user-export");
tasksService.interruptAll();            // Interrupt every registered task
tasksService.removeInterruptedTasks();  // Remove tasks that are marked interrupted
tasksService.clearTasks();              // Clear the registry
```

`interrupt(String taskName)` and `isInterrupted(String taskName)` throw `IllegalArgumentException` when the name is `null` or no registered task has that name.

## Running a Task Synchronously

For tests, command-line jobs, or small local workflows, call `executeTask()` directly.

```java
TasksService tasksService = new TasksService();
BasicTaskWrapper<Integer> task = new BasicTaskWrapper<>("numbers", 10, tasksService);

task.addProducer(new ProducerPageDTO<>(), page -> {
  page.setItems(List.of(1, 2, 3));
  page.setCompleted(true);
  return page;
});

task.addConsumer(number -> System.out.println("Consumed " + number));

task.executeTask();

boolean completed = task.isTaskCompleted();
```

The wrapper requires at least one producer and one consumer. Calling `executeTask()` without both throws `IllegalStateException`.

## Interrupting a Running Task

Interrupt through the wrapper directly:

```java
task.interrupt();
```

Or through `TasksService`:

```java
tasksService.interrupt(taskName);
```

The interrupt flag causes producers and consumers to stop their loops. During cleanup, the lifecycle runner also interrupts worker threads, shuts down the executor, and clears the queue.

Example with a background task:

```java
BasicTaskWrapper<Integer> task = new BasicTaskWrapper<>("streaming-task", 10, tasksService);

task.addProducer(new ProducerPageDTO<>(), page -> {
  page.setItems(List.of(1));
  return page;
});

task.addConsumer(item -> {
  try {
    Thread.sleep(100L);
  }
  catch (InterruptedException e) {
    Thread.currentThread().interrupt();
  }
});

Thread thread = new Thread(task::executeTask);
thread.start();

while (!tasksService.hasTask(task)) {
  Thread.sleep(1L);
}

tasksService.interrupt("streaming-task");
thread.join();
```

## Multiple Producers and Consumers

You can add several producers and several consumers to one wrapper.

```java
BasicTaskWrapper<Integer> task = new BasicTaskWrapper<>("multi-worker", 100, tasksService);

task.addProducer(new ProducerPageDTO<>("first", 50), page -> loadPage(firstSource, page));
task.addProducer(new ProducerPageDTO<>("second", 50), page -> loadPage(secondSource, page));

task.addConsumers(this::processNumber, 4);

task.executeTask();
```

The lifecycle runner uses one worker thread per producer and consumer. In the example above, it uses six worker threads.

## Consumer Failure and Restart Limit

If a consumer throws an exception, the lifecycle runner can start a replacement consumer. The default restart limit is `10`.

```java
BasicTaskWrapper<String> task = new BasicTaskWrapper<>("resilient-task", 50, tasksService);
task.setMaxConsumerRestarts(3);
```

When the restart limit is exceeded, `executeTask()` throws `IllegalStateException` with message `Consumer restart limit exceeded`, interrupts the task, clears the queue, shuts down workers, and unregisters the task.

Producer failures fail the whole task immediately.

Consumers should be idempotent when possible. A consumer receives an item before invoking your function; if your function throws after partially processing that item, the wrapper does not automatically put the item back into the queue.

## Validation Rules

- `queueSize` must be greater than zero.
- `batchSize` must be greater than zero.
- `taskName` must not be `null`.
- `addConsumers(..., instancesCount)` requires `instancesCount > 0`.
- `executeTask()` requires at least one producer and one consumer.
- `setMaxConsumerRestarts(...)` requires a non-negative value.
- Producer functions must not return `null`.
- Producer page item lists must not be `null`; use `List.of()` when a page has no items.

## Task State Methods

Each wrapper implements `TaskWrapper`:

```java
task.getTaskName();
task.executeTask();
task.interrupt();
task.isInterrupted();
task.isProducersCompleted();
task.isConsumersCompleted();
task.isTaskCompleted();
task.register(tasksService);
task.unregister(tasksService);
```

In normal use, let `executeTask()` handle registration and unregistration. Manual registration is only useful for custom lifecycle wiring.

## Choosing Queue and Batch Sizes

`queueSize` controls backpressure. A small queue limits memory use and slows producers when consumers are behind. A larger queue allows producers to get further ahead but keeps more items in memory.

For `BatchTaskWrapper`, `batchSize` controls the maximum collection size passed to each consumer call. Partial final batches are expected.

## Logging

The project uses SLF4J through Lombok's `@Slf4j`. Producers and consumers log queue activity at trace/debug/info levels. Configure logging through normal Spring Boot logging properties.
