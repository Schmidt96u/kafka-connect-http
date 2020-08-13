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
import java.util.function.Function;

public class ListParseToRecordsParser implements HttpResponseParser {
  private final Function<Map<String, ?>, ListParseToRecordsConfig> configFactory;
  private String path;
  @Override
  public List<SourceRecord> parse(HttpResponse response) {

  }
  @Override
  public void configure(Map<String, ?> configs) {
    ListParseToRecordsConfig config = configFactory.apply(configs);
    path = config.getPath();
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
