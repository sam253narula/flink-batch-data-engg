package com.learning;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

/* Compute total score, use student_scores.csv
total score= class score + test Score
 */
public class ComputerTotalScoreOfEachStudent {

    public static void main(String[] args) throws Exception {
        Utils.printHeader("Read CSV file");
        //Get the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<String, String, Double, Double>> student_scores = env.readCsvFile("src/main/resources/student_scores.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('\"')
                .types(String.class, String.class, Double.class, Double.class);
        student_scores.print();
        DataSet<Tuple5<String, String, Double, Double, Double>> computedTotalScores = student_scores.map(new MapComputeTotalScoreOfEachStudent());
        Utils.printHeader("Read Computed Total Scores");
        computedTotalScores.print();
    }

}
