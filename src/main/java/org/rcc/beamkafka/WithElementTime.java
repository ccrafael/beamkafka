package org.rcc.beamkafka;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import scala.Tuple2;


public class WithElementTime extends DoFn<KV<String, Integer>, KV<String, Tuple2<Integer, Instant>>> {
    @ProcessElement
    public void processElement(ProcessContext c, @Element KV<String, Integer> element) {

        c.output(KV.of(element.getKey(), new Tuple2<>(element.getValue(), c.timestamp())));
    }
}

