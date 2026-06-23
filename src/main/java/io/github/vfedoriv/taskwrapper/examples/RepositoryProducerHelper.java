package io.github.vfedoriv.taskwrapper.examples;

import io.github.vfedoriv.taskwrapper.producer.ProducerPageDTO;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RepositoryProducerHelper
{
  private final ExampleRepository exampleRepository;

  public RepositoryProducerHelper(
      final ExampleRepository exampleRepository)
  {
    this.exampleRepository = Objects.requireNonNull(exampleRepository, "Example repository is required");
  }

  public ProducerPageDTO<ExampleItem> produceExampleItems(final ProducerPageDTO<ExampleItem> pageDTO) {
    return produceItems(pageDTO, exampleRepository::findExampleItemsByRange);
  }

  private static <T> ProducerPageDTO<T> produceItems(
      final ProducerPageDTO<T> pageDTO,
      final BiFunction<String, Integer, List<T>> repositoryBiFunction)
  {
    Objects.requireNonNull(pageDTO, "Producer page is required");
    Objects.requireNonNull(repositoryBiFunction, "Repository function is required");

    if (pageDTO.getCounter() != 0 && pageDTO.getLastItem() == null) {
      pageDTO.setCompleted(true);
      pageDTO.setItems(Collections.emptyList());
      return pageDTO;
    }

    String id = getIterationId(pageDTO);
    List<T> items = Objects.requireNonNull(
        repositoryBiFunction.apply(id, pageDTO.getPageSize()),
        "Repository function returned null");
    log.debug("Fetched {} items", items.size());

    pageDTO.setItems(items);
    if (!items.isEmpty()) {
      pageDTO.setLastItem(items.get(items.size() - 1));
    }
    else {
      pageDTO.setLastItem(null);
      pageDTO.setCompleted(true);
      log.debug("Processed {} items", pageDTO.getIteratedValue());
    }

    pageDTO.incrementCounter();
    if (pageDTO.getIteratedValue() == null) {
      pageDTO.setIteratedValue(0);
    }
    Integer itemsCount = (Integer) pageDTO.getIteratedValue() + items.size();
    pageDTO.setIteratedValue(itemsCount);
    return pageDTO;
  }

  private static <T> String getIterationId(final ProducerPageDTO<T> pageDTO) {
    String id;
    if (pageDTO.getCounter() == 0) {
      id = pageDTO.getId();
    }
    else if (pageDTO.getLastItem() instanceof ExampleItem exampleItem) {
      id = exampleItem.getId();
    }
    else {
      log.error("Unknown type of item provided: {}", pageDTO.getLastItem().getClass().getSimpleName());
      throw new IllegalArgumentException(
          "Cannot return item Id for type " + pageDTO.getLastItem().getClass().getSimpleName());
    }

    if (id == null || id.isEmpty()) {
      log.error("Empty or null item id provided");
      throw new IllegalArgumentException("Empty or null item id provided");
    }
    return id;
  }
}
