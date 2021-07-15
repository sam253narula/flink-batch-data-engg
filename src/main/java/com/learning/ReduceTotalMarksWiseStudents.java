package com.learning;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class ReduceTotalMarksWiseStudents implements ReduceFunction<Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> reduce(Tuple2<String, Double> rec1, Tuple2<String, Double> rec2) throws Exception {
        return new Tuple2(rec1.f0, rec1.f1+rec2.f1);
    }
}
