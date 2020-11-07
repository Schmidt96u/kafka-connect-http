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

import javax.xml.transform.Source;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static org.apache.commons.lang.SerializationUtils.deserialize;

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
        return (parseHandle(deserialize(response.getBody()), new ArrayList<String>(Arrays.asList(this.path.split(".")))));
      }

  private List<SourceRecord> parseHandle(JsonNode node, ArrayList<String> paths) {
      int n = 0;
      String subPath ="/";
      ArrayList<SourceRecord> sourceRecords = new ArrayList<>();
      while(!subPath.endsWith("[]") && n < paths.size()){
       subPath = subPath.concat(paths.get(n));
       n++;
      }

      if (n == paths.size()) {
        if (subPath.endsWith("[]")) {
          ArrayNode arrayNode = (ArrayNode) node;
          for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode arrayElement = arrayNode.get(i);
            sourceRecords.add(new SourceRecord(arrayElement)));
          }
        }
          else if(!subPath.contains("[]")){
            JsonNode nodeObject = node.at(subPath);
            return new SourceRecord(/*have to findout how i'm gonna do that ... */);
          }
      }
      else if(subPath.endsWith("[]")){
        ArrayNode arrayNode = (ArrayNode) node;
        for (int i = 0; i < arrayNode.size(); i++) {
          JsonNode arrayElement = arrayNode.get(i);
          sourceRecords.addAll(parseHandle(arrayElement, (ArrayList<String>) paths.subList(n,paths.size()-1)));
      } }
    return sourceRecords;
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
