/**
 * Created by alyona on 16.12.15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

public class TestData {


        private static final String TASK_NAME = "Aggregation task";
        private static final String CHANNEL_SHOW_FILE = "file1.txt";
        private static final String SHOW_AMOUNT_FILE = "file2.txt";
        private static final String RESULT_FILE = "result";

        public static void main(String[] args) {
            SparkConf conf = new SparkConf().setAppName(TASK_NAME);
            JavaSparkContext sc = new JavaSparkContext(conf);

            JavaRDD<String> channelShowRDD = sc.textFile(CHANNEL_SHOW_FILE);
            JavaRDD<String> showAmountRDD = sc.textFile(SHOW_AMOUNT_FILE);


        }

}


