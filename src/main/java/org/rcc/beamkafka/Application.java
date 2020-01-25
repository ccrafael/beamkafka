package org.rcc.beamkafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Application {


    public static void main(String[] args) {

        PipelineOptionsFactory.register(Options.class);
        PipelineOptions op = PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
        Pipeline p = Pipeline.create(op);

        Options options = op.as(Options.class);

        System.out.println("read from servers " + options.getBootstrapServers());
        System.out.println("read from topic [" + options.getTopicName() + "]");

        Window<KV<String, Tuple2<Integer, Instant>>> window = Window.<KV<String, Tuple2<Integer, Instant>>>into(FixedWindows.of(Duration.standardSeconds(20)))
                //.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(10)))

                .withAllowedLateness(Duration.standardSeconds(0))
                .discardingFiredPanes();

        Map<String, Object> properties = new HashMap<String, Object>() {{
            put("group.id", "test");
            put("auto.offset.reset", "latest");
        }};

        PTransform<PBegin, PCollection<KV<String, Integer>>> kafkaReader = KafkaIO.<String, Integer>read()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopic(options.getTopicName())
                .withConsumerConfigUpdates(properties)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(IntegerDeserializer.class)
                .withoutMetadata();


        p.apply(kafkaReader)
                .apply(ParDo.of(new WithElementTime()))

                .apply(window)

                .apply(Combine.perKey(new BasicCombine()))

                //.apply(Filter.by(kv -> kv.getValue() != null))

                .apply(ParDo.of(new WriteToConsole("Combine1")))

                .apply(window)

                .apply(Combine.perKey(new BasicCombine()))
                .apply(ParDo.of(new WriteToConsole("Combine2")))
                .apply(MapElements.into(strings()).via(String::valueOf))
                .apply(TextIO.write()
                        .to("tmp.data")
                        .withNumShards(1).withWindowedWrites());


        PipelineResult result = p.run();

        System.out.println("wait until finish");

        while (!result.waitUntilFinish(Duration.standardSeconds(10)).isTerminal()) {
//            System.out.println(" metrics: " + result.metrics().allMetrics());
        }

        System.out.println("Finish");
    }


}
