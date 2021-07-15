package com.learning;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class ComputeEachStudentTotalScoreForPhysics {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<String, String, Double, Double>> student_scores = env.readCsvFile("src/main/resources/student_scores.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('\"')
                .types(String.class, String.class, Double.class, Double.class);

        DataSet<Tuple5<String, String, Double, Double, Double>> computedTotalScores = student_scores.map(new MapComputeTotalScoreOfEachStudent());
        DataSet<Tuple2<String, Double>> StudentNameToPhysicsTotalScore = computedTotalScores
                .map(column -> Tuple3.of(column.f0, column.f1, column.f4))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE))
                .filter(new FilterBySubjectName())
                .map(column -> Tuple2.of(column.f0, column.f2))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
        Utils.printHeader("Student Name and their total score in Physics");
        StudentNameToPhysicsTotalScore.print();
    }

}
