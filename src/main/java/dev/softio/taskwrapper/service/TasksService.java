package dev.softio.taskwrapper.service;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import dev.softio.taskwrapper.model.TaskWrapper;

import org.springframework.stereotype.Component;

@Component
public class TasksService
{
  private final ConcurrentHashMap<String, TaskWrapper> tasks = new ConcurrentHashMap<>();

  public void addTask(final TaskWrapper task) {
    Objects.requireNonNull(task, "Cannot add null task");
    if (tasks.putIfAbsent(task.getTaskName(), task) != null) {
      throw new IllegalStateException("Task already in task list: " + task.getTaskName());
    }
  }

  public void removeTask(final TaskWrapper task) {
    if (task != null) {
      tasks.remove(task.getTaskName(), task);
    }
  }

  public Collection<TaskWrapper> getTasks() {
    return tasks.values();
  }

  public void clearTasks() {
    tasks.clear();
  }

  public boolean hasTask(final TaskWrapper task) {
    if (task == null) {
      return false;
    }
    return tasks.containsKey(task.getTaskName());
  }

  public void interrupt(final String taskName) {
    TaskWrapper task = getTaskOrThrow(taskName);
    task.interrupt();
  }

  private TaskWrapper getTaskOrThrow(final String taskName) {
    if (taskName == null) {
      throw new IllegalArgumentException("Task name is required");
    }
    TaskWrapper task = tasks.get(taskName);
    if (task == null) {
      throw new IllegalArgumentException("Task with provided name does not exist: " + taskName);
    }
    return task;
  }

  public boolean isInterrupted(final String taskName) {
    return getTaskOrThrow(taskName).isInterrupted();
  }

  public void removeInterruptedTasks() {
    tasks.forEach((key, value) -> {
      if (value.isInterrupted()) {
        tasks.remove(key, value);
      }
    });
  }

  public void interruptAll() {
    tasks.forEach((key, value) -> value.interrupt());
  }

}
