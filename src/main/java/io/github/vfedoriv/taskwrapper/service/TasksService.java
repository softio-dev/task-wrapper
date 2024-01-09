package io.github.vfedoriv.taskwrapper.service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import io.github.vfedoriv.taskwrapper.model.TaskWrapper;

import org.springframework.stereotype.Component;

@Component
public class TasksService
{
  private final ConcurrentHashMap<String, TaskWrapper> tasks = new ConcurrentHashMap<>();

  public void addTask(final TaskWrapper task) {
    if (task == null) {
      throw new RuntimeException("Cannot add - task is null");
    }
    if (tasks.putIfAbsent(task.getTaskName(), task) != null) {
      throw new RuntimeException("Task already in task list!");
    }
  }

  public void removeTask(final TaskWrapper task) {
    if (task != null) {
      tasks.remove(task.getTaskName());
    }
  }

  public Collection<TaskWrapper> getTasks() {
    return tasks.values();
  }

  public void clearTasks() {
    tasks.clear();
  }

  public boolean hasTask(final TaskWrapper task) {
    return tasks.containsKey(task.getTaskName());
  }

  public void interrupt(final String taskName) {
    if (tasks.containsKey(taskName)) {
      tasks.get(taskName).interrupt();
    }
  }

  public boolean isInterrupted(final String taskName) {
    if (tasks.containsKey(taskName)) {
      return tasks.get(taskName).isInterrupted();
    }
    throw new RuntimeException("Task with provided name not exist!");
  }

  public void removeInterruptedTasks() {
    tasks.forEach( (key, value) -> {
      if (value.isInterrupted()) {
        tasks.remove(key);
      }
    });
  }

  public void interruptAll() {
    tasks.forEach( (key, value) -> {
      value.interrupt();
      });
  }

}
