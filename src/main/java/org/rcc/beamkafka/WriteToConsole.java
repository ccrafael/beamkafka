package org.rcc.beamkafka;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import scala.Tuple2;

import static java.time.OffsetDateTime.now;

public class WriteToConsole extends DoFn<KV<String, Tuple2<Integer, Instant>>, KV<String, Tuple2<Integer, Instant>>> {
    private final String prefix;

    WriteToConsole(String prefix) {
        this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @Element KV<String, Tuple2<Integer, Instant>> element) {

        System.out.println(log(element, c.timestamp()));
        c.output(element);
    }

    private String log(KV<String, Tuple2<Integer, Instant>> element, Instant timestamp) {
        if (element.getValue() != null) {
            return String.format("[%s-%s] kv: (%8s,%8d)  element time: (original, final): (%s, %s)",
                    now(),
                    prefix,
                    element.getKey(),
                    element.getValue()._1,
                    element.getValue()._2,
                    timestamp);
        } else {
            return String.format("[%s-%s] kv: (%8s,--------)  element time: (original, final): (------------------------, %s)",
                    now(),
                    prefix,
                    element.getKey(),
                    timestamp);
        }
    }

}
