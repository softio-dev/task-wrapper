package io.github.vfedoriv.taskwrapper.producer;

import java.util.List;
import java.util.ArrayList;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ProducerPageDTO<T>
{
  private String id;

  private int pageSize;

  private int counter;

  private Object iteratedValue;

  private T lastItem;

  private List<T> items = new ArrayList<>();

  private boolean completed;

  public ProducerPageDTO(
      String id,
      int pageSize)
  {
    this.id = id;
    this.pageSize = pageSize;
  }

  public void incrementCounter() {
    this.counter++;
  }
}
