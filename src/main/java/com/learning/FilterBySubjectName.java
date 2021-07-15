package com.learning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.*;

public class FilterBySubjectName implements FilterFunction<Tuple3<String,String, Double>> {
    @Override
    public boolean filter(Tuple3<String,String, Double> rec) throws Exception {
        return rec.f1.toString().equals("Physics") ;
    }
}
