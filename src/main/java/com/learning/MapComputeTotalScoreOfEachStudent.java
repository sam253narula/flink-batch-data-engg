package com.learning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class MapComputeTotalScoreOfEachStudent implements MapFunction<Tuple4<String, String, Double, Double>, //input tuple
        Tuple5<String, String, Double, Double, Double> //output tuple
        > {
    @Override
    public Tuple5<String, String, Double, Double, Double> map(Tuple4<String, String, Double, Double> student_scores) throws Exception {
        return new Tuple5<>(student_scores.f0,student_scores.f1, student_scores.f2, student_scores.f3,
                //Compute the total score
                (student_scores.f2+ student_scores.f3 ));
    }
}
