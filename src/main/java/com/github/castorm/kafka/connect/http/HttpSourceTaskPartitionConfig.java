package com.github.castorm.kafka.connect.http;

/*-
 * #%L
 * kafka-connect-http
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

import com.github.castorm.kafka.connect.http.client.okhttp.OkHttpClient;
import com.github.castorm.kafka.connect.http.client.spi.HttpClient;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.model.Partition;
import com.github.castorm.kafka.connect.http.record.OffsetRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.OrderDirectionSourceRecordSorter;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordSorter;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.request.template.TemplateHttpRequestFactory;
import com.github.castorm.kafka.connect.http.response.PolicyHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.timer.AdaptableIntervalTimer;
import com.github.castorm.kafka.connect.timer.spi.Timer;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownMap;
import static com.github.castorm.kafka.connect.common.ConfigUtils.replaceKey;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Getter
public class HttpSourceTaskPartitionConfig extends AbstractConfig {

    private static final String TIMER = "http.timer";
    private static final String CLIENT = "http.client";
    private static final String INITIAL_OFFSET = "http.offset.initial";
    private static final String REQUEST_FACTORY = "http.request.factory";
    private static final String RESPONSE_PARSER = "http.response.parser";
    private static final String RECORD_SORTER = "http.record.sorter";
    private static final String RECORD_FILTER_FACTORY = "http.record.filter.factory";

    private final Partition partition;
    private final Timer timer;
    private final HttpRequestFactory requestFactory;
    private final HttpClient client;
    private final HttpResponseParser responseParser;
    private final SourceRecordFilterFactory recordFilterFactory;
    private final SourceRecordSorter recordSorter;
    private final Offset initialOffset;

    HttpSourceTaskPartitionConfig(Partition partition, Map<String, ?> originals) {
        super(config(), getPartitionOverrides(partition.getName(), originals));
        this.partition = partition;
        timer = getConfiguredInstance(TIMER, Timer.class);
        requestFactory = getConfiguredInstance(REQUEST_FACTORY, HttpRequestFactory.class);
        client = getConfiguredInstance(CLIENT, HttpClient.class);
        responseParser = getConfiguredInstance(RESPONSE_PARSER, HttpResponseParser.class);
        recordSorter = getConfiguredInstance(RECORD_SORTER, SourceRecordSorter.class);
        recordFilterFactory = getConfiguredInstance(RECORD_FILTER_FACTORY, SourceRecordFilterFactory.class);
        initialOffset = Offset.of(breakDownMap(getString(INITIAL_OFFSET)));
    }

    private static Map<String, ?> getPartitionOverrides(String name, Map<String, ?> originals) {
        return replaceKey("partitions." + name, "", originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(TIMER, CLASS, AdaptableIntervalTimer.class, HIGH, "Poll Timer Class")
                .define(CLIENT, CLASS, OkHttpClient.class, HIGH, "Request Client Class")
                .define(REQUEST_FACTORY, CLASS, TemplateHttpRequestFactory.class, HIGH, "Request Factory Class")
                .define(RESPONSE_PARSER, CLASS, PolicyHttpResponseParser.class, HIGH, "Response Parser Class")
                .define(RECORD_SORTER, CLASS, OrderDirectionSourceRecordSorter.class, LOW, "Record Sorter Class")
                .define(RECORD_FILTER_FACTORY, CLASS, OffsetRecordFilterFactory.class, LOW, "Record Filter Factory Class")
                .define(INITIAL_OFFSET, STRING, "", HIGH, "Starting offset");
    }
}