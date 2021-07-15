package com.learning;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import scala.Tuple2;

public class ComputeTopStudentForEachSubject {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<String, String, Double, Double>> student_scores = env.readCsvFile("src/main/resources/student_scores.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('\"')
                .types(String.class, String.class, Double.class, Double.class);
        DataSet<Tuple5<String, String, Double, Double, Double>> computedTotalScores = student_scores.map(new MapComputeTotalScoreOfEachStudent());
        DataSet<Tuple3<String, String, Double>> topStudentBySubject =
        computedTotalScores.map(i -> Tuple3.of(i.f0,i.f1,i.f4))
                .returns(Types.TUPLE(Types.STRING,Types.STRING,Types.DOUBLE))
                .groupBy(1)
                .reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> reduce(Tuple3<String, String, Double> rec1, Tuple3<String, String, Double> rec2) throws Exception {
                        if(rec1.f2 >rec2.f2){
                            return rec1;
                        }
                        else{
                            return  rec2;
                        }
                    }
                });
        topStudentBySubject.print();

    }

}
