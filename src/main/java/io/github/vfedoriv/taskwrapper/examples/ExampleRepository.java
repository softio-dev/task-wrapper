package io.github.vfedoriv.taskwrapper.examples;

import java.util.List;

import org.springframework.stereotype.Component;

@Component
public class ExampleRepository
{
  private final List<ExampleItem> items = List.of(
      new ExampleItem("item-001", "First example item"),
      new ExampleItem("item-002", "Second example item"),
      new ExampleItem("item-003", "Third example item"),
      new ExampleItem("item-004", "Fourth example item"),
      new ExampleItem("item-005", "Fifth example item"));

  public List<ExampleItem> findExampleItemsByRange(
      final String lastSeenId,
      final Integer pageSize)
  {
    if (pageSize == null || pageSize <= 0) {
      throw new IllegalArgumentException("Page size must be greater than zero");
    }

    return items.stream()
        .filter(item -> item.getId().compareTo(lastSeenId) > 0)
        .limit(pageSize)
        .toList();
  }
}
