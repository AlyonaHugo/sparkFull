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

            JavaPairRDD<String, String> showChannelMap = channelShowRDD
                    .mapToPair(splitStringInMap());
            showChannelMap.collect().forEach(System.out::println);
            System.out.println("----------end of show channel map---------");


        }



    private static PairFunction<String, String, String> splitStringInMap() {
        return new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                int channel = 0;
                int show = 1;
                String[] mas = s.split(",");
                return new Tuple2<String, String>(mas[show], mas[channel]);
            }
        };
    }
}


