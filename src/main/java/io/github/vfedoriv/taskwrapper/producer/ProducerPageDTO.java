package io.github.vfedoriv.taskwrapper.producer;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ProducerPageDTO<T>
{
  private String purl;

  private int pageSize;

  private int counter;

  private Object iteratedValue;

  private T lastItem;

  private List<T> items;

  private boolean completed;

  public ProducerPageDTO(
      String purl,
      int pageSize)
  {
    this.purl = purl;
    this.pageSize = pageSize;
  }

  public void incrementCounter() {
    this.counter++;
  }
}
