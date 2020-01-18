package org.rcc.beamkafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Application {

    public static class Acc implements Serializable {
        Integer data;
    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(Options.class);
        PipelineOptions op = PipelineOptionsFactory.fromArgs(args).withoutStrictParsing().create();
        Pipeline p = Pipeline.create(op);

        Options options = op.as(Options.class);

        System.out.println("read from servers " + options.getBootstrapServers());
        System.out.println("read from topic [" + options.getTopicName() + "]");

        Window<KV<String, Integer>> window = Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(10)))
                //.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(100000000)))
                //.discardingFiredPanes()
                .withAllowedLateness(Duration.standardSeconds(0));

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
                .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, @Element KV<String, Integer> element, OutputReceiver<KV<String, Integer>> out) {
                        //System.out.println("input key: [" + element.getKey() + "], value: [" + element.getValue() + "] \t\t event time: " + c.timestamp());
                        out.output(element);
                    }
                }))

                .apply(window)

                .apply(Combine.perKey(new CombineFn<Integer, Acc, Integer>() {
                    @Override
                    public Acc createAccumulator() {
                        Acc data = new Acc();
                        data.data = null;

                        return data;
                    }

                    private Integer reduce(Integer a, Integer b) {
                        if (a == null) {
                            return b;
                        } else if (b == null) {
                            return a;
                        } else if (a > b) {
                            return a;
                        } else {
                            return b;
                        }
                    }

                    @Override
                    public Acc addInput(Acc acc, Integer input) {
                        acc.data = reduce(acc.data, input);
                        return acc;
                    }

                    @Override
                    public Acc mergeAccumulators(Iterable<Acc> accumulators) {
                        Acc acc = createAccumulator();
                        System.out.println("merge: " + accumulators);
                        acc.data = StreamSupport.stream(accumulators.spliterator(), false).map(a -> a.data)
                                .reduce(null, this::reduce);

                        return acc;
                    }

                    @Override
                    public Integer extractOutput(Acc accumulator) {
                        //System.out.println( "ExtractOutput: " + accumulator.data);
                        return accumulator.data;
                    }
                }))

                .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c, @Element KV<String, Integer> element, OutputReceiver<KV<String, Integer>> out) {
                        System.out.println("output key: [" + element.getKey() + "], value: [" + element.getValue() + "] \t\t event time: " + c.timestamp());
                        out.output(element);
                    }

                }))

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
