package dev.softio.taskwrapper.model;

import dev.softio.taskwrapper.service.TasksService;

public interface TaskWrapper
{
  public void executeTask();

  public void interrupt();

  public boolean isInterrupted();

  public boolean isProducersCompleted();

  public boolean isConsumersCompleted();

  public boolean isTaskCompleted();

  public String getTaskName();

  public void register(final TasksService tasksService);

  public void unregister(final TasksService tasksService);
}
