package org.rcc.beamkafka;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.joda.time.Instant;
import org.rcc.beamkafka.BasicCombine.Acc;
import scala.Tuple2;

import java.io.Serializable;
import java.util.stream.StreamSupport;

public class BasicCombine extends CombineFn<Tuple2<Integer, Instant>, Acc, Tuple2<Integer, Instant>> {

    static class Acc implements Serializable {
        Tuple2<Integer, Instant> data;
    }

    @Override
    public Acc createAccumulator() {
        Acc data = new Acc();
        data.data = null;

        return data;
    }

    private Tuple2<Integer, Instant> reduce(Tuple2<Integer, Instant> a, Tuple2<Integer, Instant> b) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        } else if (a._1 > b._1) {
            return a;
        } else {
            return b;
        }
    }

    @Override
    public Acc addInput(Acc acc, Tuple2<Integer, Instant> input) {
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
    public Tuple2<Integer, Instant> extractOutput(Acc accumulator) {
        //System.out.println( "ExtractOutput: " + accumulator.data);
        return accumulator.data;
    }
}

