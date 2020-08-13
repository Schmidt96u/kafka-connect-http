package com.github.castorm.kafka.connect.http.response.ListParser;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 CastorM
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@RequiredArgsConstructor
public class ListParseToRecordsParser implements HttpResponseParser {
  private final Function<Map<String, ?>, ListParseToRecordsConfig> configFactory;
  private final ObjectMapper objectMapper;
  private String path;

  public ListParseToRecordsParser() {
    this(ListParseToRecordsConfig::new,new ObjectMapper());
  }

  @Override
  public List<SourceRecord> parse(HttpResponse response) {
        return (parseHandle(deserialize(response.getBody()),this.path,this.path.split("\\[\\]").length-1));
      }

  private List<SourceRecord> parseHandle(JsonNode node, String path,Integer level) {
    if (level == 0) {
      if (node.isObject()) {
        Iterator<String> fieldNames = node.fieldNames();

        while (fieldNames.hasNext()) {
          String fieldName = fieldNames.next();
          JsonNode fieldValue = node.get(fieldName);
          parseHandle(fieldValue);
        }
      } else if (node.isArray()) {
        ArrayNode arrayNode = (ArrayNode) node;
        for (int i = 0; i < arrayNode.size(); i++) {
          JsonNode arrayElement = arrayNode.get(i);
          parseHandle(arrayElement);
        }
      } else {

      }
    } else {
      if (node.isObject() &&) {
        Iterator<String> fieldNames = node.fieldNames();

        while (fieldNames.hasNext()) {
          String fieldName = fieldNames.next();
          JsonNode fieldValue = node.get(fieldName);
          parseHandle(fieldValue);
        }
      } else if (node.isArray()) {
        ArrayNode arrayNode = (ArrayNode) node;
        for (int i = 0; i < arrayNode.size(); i++) {
          JsonNode arrayElement = arrayNode.get(i);
          parseHandle(arrayElement);
        }
      } else {

      }
    }
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

  @SneakyThrows(IOException.class)
  private JsonNode deserialize(byte[] body) throws IOException {
    return objectMapper.readTree(body);
  }

}
