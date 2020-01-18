package org.rcc.beamkafka;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface Options extends PipelineOptions {

    @Description("Topic name")
    String getTopicName();
    void setTopicName(String topicName);

    @Description("Kafka bootstrap servers spited by semicolon.")
    String getBootstrapServers();

    void setBootstrapServers(String bootstrapServers);
}
