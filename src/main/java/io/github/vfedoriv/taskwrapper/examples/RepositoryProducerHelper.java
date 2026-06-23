package io.github.vfedoriv.taskwrapper.examples;

import io.github.vfedoriv.taskwrapper.producer.ProducerPageDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

@Slf4j
@Component
public class RepositoryProducerHelper {
    private final ExampleRepository exampleRepository;

    public RepositoryProducerHelper(
            final ExampleRepository exampleRepository)
    {
        this.exampleRepository = exampleRepository;
    }

    public ProducerPageDTO<ExampleItem> produceExampleItems(final ProducerPageDTO<ExampleItem> pageDTO) {
        return produceItems(pageDTO, exampleRepository::findExampleItemsByRange);
    }

    private static <T> ProducerPageDTO<T> produceItems(
            final ProducerPageDTO<T> pageDTO,
            final BiFunction<String, Integer, List<T>> repositoryBiFunction)
    {
        if (pageDTO.getCounter() != 0 && pageDTO.getLastItem() == null) {
            pageDTO.setCompleted(true);
            pageDTO.setItems(Collections.emptyList());
            return pageDTO;
        }
        String id = getIterationId(pageDTO);
        List<T> items = repositoryBiFunction.apply(id, pageDTO.getPageSize());
        log.debug("Fetched {} items", items.size());
        pageDTO.setItems(items);
        if (!items.isEmpty()) {
            pageDTO.setLastItem(items.get(items.size() - 1));
        }
        else {
            pageDTO.setLastItem(null);
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
            // first iteration through collection
            id = pageDTO.getId();
        }
        else { // continue from place where previous iteration ended
            if (pageDTO.getLastItem() instanceof ExampleItem ) {
                id = ((ExampleItem) pageDTO.getLastItem()).getId();
            }
            else {
                log.error("Unknown type of item provided: {}", pageDTO.getLastItem().getClass().getSimpleName());
                throw new RuntimeException(
                        "Cannot return item Id for type " + pageDTO.getLastItem().getClass().getSimpleName());
            }
        }
        if (id == null || id.isEmpty()) {
            log.error("Empty or null item id provided");
            throw new RuntimeException("Empty or null item id provided");
        }
        return id;
    }

}
