package io.github.vfedoriv.taskwrapper.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vfedoriv.taskwrapper.model.TaskWrapper;

import org.junit.jupiter.api.Test;

class TasksServiceTests
{
  @Test
  void addsAndRemovesTask() {
    TasksService tasksService = new TasksService();
    StubTask task = new StubTask("task");

    tasksService.addTask(task);
    assertTrue(tasksService.hasTask(task));

    tasksService.removeTask(task);
    assertFalse(tasksService.hasTask(task));
  }

  @Test
  void rejectsDuplicateTaskNames() {
    TasksService tasksService = new TasksService();

    tasksService.addTask(new StubTask("task"));

    assertThrows(IllegalStateException.class, () -> tasksService.addTask(new StubTask("task")));
  }

  @Test
  void rejectsNullTask() {
    TasksService tasksService = new TasksService();

    assertThrows(NullPointerException.class, () -> tasksService.addTask(null));
  }

  @Test
  void interruptByNameMarksTaskInterrupted() {
    TasksService tasksService = new TasksService();
    StubTask task = new StubTask("task");
    tasksService.addTask(task);

    tasksService.interrupt("task");

    assertTrue(task.isInterrupted());
    assertTrue(tasksService.isInterrupted("task"));
  }

  @Test
  void missingTaskNameThrowsConsistently() {
    TasksService tasksService = new TasksService();

    assertThrows(IllegalArgumentException.class, () -> tasksService.interrupt("missing"));
    assertThrows(IllegalArgumentException.class, () -> tasksService.isInterrupted("missing"));
  }

  @Test
  void interruptAllAndRemoveInterruptedTasks() {
    TasksService tasksService = new TasksService();
    StubTask first = new StubTask("first");
    StubTask second = new StubTask("second");
    tasksService.addTask(first);
    tasksService.addTask(second);

    tasksService.interruptAll();
    tasksService.removeInterruptedTasks();

    assertFalse(tasksService.hasTask(first));
    assertFalse(tasksService.hasTask(second));
  }

  @Test
  void removeTaskDoesNotRemoveDifferentTaskWithSameName() {
    TasksService tasksService = new TasksService();
    StubTask registered = new StubTask("task");
    StubTask different = new StubTask("task");
    tasksService.addTask(registered);

    tasksService.removeTask(different);

    assertTrue(tasksService.hasTask(registered));
  }

  @Test
  void removingNullTaskIsNoOp() {
    TasksService tasksService = new TasksService();

    assertDoesNotThrow(() -> tasksService.removeTask(null));
  }

  private static final class StubTask implements TaskWrapper
  {
    private final String taskName;

    private boolean interrupted;

    private StubTask(final String taskName) {
      this.taskName = taskName;
    }

    @Override
    public void executeTask() {
    }

    @Override
    public void interrupt() {
      interrupted = true;
    }

    @Override
    public boolean isInterrupted() {
      return interrupted;
    }

    @Override
    public boolean isProducersCompleted() {
      return false;
    }

    @Override
    public boolean isConsumersCompleted() {
      return false;
    }

    @Override
    public boolean isTaskCompleted() {
      return false;
    }

    @Override
    public String getTaskName() {
      return taskName;
    }

    @Override
    public void register(final TasksService tasksService) {
    }

    @Override
    public void unregister(final TasksService tasksService) {
    }
  }
}
