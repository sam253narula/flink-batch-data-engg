package com.learning;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class ComputeTotalAvgScoreForEachStudent {
    public static void main(String[] args) throws Exception {


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<String, String, Double, Double>> student_scores = env.readCsvFile("src/main/resources/student_scores.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('\"')
                .types(String.class, String.class, Double.class, Double.class);

        DataSet<Tuple5<String, String, Double, Double, Double>> computedTotalScores = student_scores.map(new MapComputeTotalScoreOfEachStudent());
        DataSet<Tuple2<String,Double>> computedTotalScoreForEachStudent =
        computedTotalScores.map(i -> Tuple2.of(i.f0,i.f4))   //selected subset of columns
                .returns(Types.TUPLE(Types.STRING,Types.DOUBLE))         // set return types
                .groupBy(0)
                .reduce(new ReduceTotalMarksWiseStudents());
        computedTotalScoreForEachStudent.print();

    }
}
