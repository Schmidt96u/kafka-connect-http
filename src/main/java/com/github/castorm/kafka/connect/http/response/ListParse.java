package com.github.castorm.kafka.connect.http.response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public class ListParse implements HttpResponseParser {
  private String path;
  private String CONFIGURE_PATH="path";
  @Override
  public List<SourceRecord> parse(HttpResponse response) {
    if(path.contains("[]")){

    }

  }

  @Override
  public void configure(Map<String, ?> map) {
    path = requireString(map.get(CONFIGURE_PATH));
  }

  public static String requireString(Object value) {
    if (!(value instanceof String)) {
      throw new DataException(
          "Only String objects supported for path, found: "
              + nullSafeClassName(
              value));
    }
    return (String) value;
  }
  private static String nullSafeClassName(Object x) {
    return x == null ? "null" : x.getClass().getName();
  }
}
