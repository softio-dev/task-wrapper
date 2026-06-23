package io.github.vfedoriv.taskwrapper.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import io.github.vfedoriv.taskwrapper.producer.ProducerPageDTO;

import org.junit.jupiter.api.Test;

class RepositoryProducerHelperTests
{
  private final RepositoryProducerHelper helper = new RepositoryProducerHelper(new ExampleRepository());

  @Test
  void producesRepositoryItemsByIdRange() {
    ProducerPageDTO<ExampleItem> page = new ProducerPageDTO<>("item-000", 2);

    helper.produceExampleItems(page);
    assertIds(page.getItems(), "item-001", "item-002");
    assertEquals("item-002", page.getLastItem().getId());
    assertEquals(1, page.getCounter());
    assertEquals(2, page.getIteratedValue());
    assertFalse(page.isCompleted());

    helper.produceExampleItems(page);
    assertIds(page.getItems(), "item-003", "item-004");
    assertEquals("item-004", page.getLastItem().getId());
    assertEquals(2, page.getCounter());
    assertEquals(4, page.getIteratedValue());
    assertFalse(page.isCompleted());
  }

  @Test
  void completesWhenRepositoryReturnsEmptyPage() {
    ProducerPageDTO<ExampleItem> page = new ProducerPageDTO<>("item-004", 2);

    helper.produceExampleItems(page);
    assertIds(page.getItems(), "item-005");
    assertFalse(page.isCompleted());

    helper.produceExampleItems(page);
    assertTrue(page.getItems().isEmpty());
    assertTrue(page.isCompleted());
    assertEquals(2, page.getCounter());
    assertEquals(1, page.getIteratedValue());
  }

  @Test
  void rejectsEmptyInitialId() {
    ProducerPageDTO<ExampleItem> page = new ProducerPageDTO<>("", 2);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> helper.produceExampleItems(page));

    assertEquals("Empty or null item id provided", exception.getMessage());
  }

  private static void assertIds(
      final List<ExampleItem> items,
      final String... ids)
  {
    assertEquals(List.of(ids), items.stream().map(ExampleItem::getId).toList());
  }
}
