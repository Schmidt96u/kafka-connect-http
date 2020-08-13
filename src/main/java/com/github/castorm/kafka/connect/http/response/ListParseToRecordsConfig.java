package com.github.castorm.kafka.connect.http.response;

import com.github.castorm.kafka.connect.http.record.SchemedKvSourceRecordMapper;
import com.github.castorm.kafka.connect.http.record.spi.KvSourceRecordMapper;
import com.github.castorm.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponsePolicy;
import com.github.castorm.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Getter
public class ListParseToRecordsConfig extends AbstractConfig {

  private static final String PARSER_DELEGATE = "http.response.parser";
  private static final String PATH = "http.response.path";

  private final HttpResponseParser delegateParser;
  private final String path;

  public ListParseToRecordsConfig(Map<String, ?> originals) {
    super(config(), originals);
    delegateParser = getConfiguredInstance(PARSER_DELEGATE, ListParseToRecordsParser.class);
    path = getString(PATH);
  }

  public String getPath() {
    return path;
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(PARSER_DELEGATE, CLASS, ListParseToRecordsParser.class, HIGH, "Response Parser Delegate Class")
        .define(PATH, STRING, "", HIGH, "Path to choose");
  }
}

}
