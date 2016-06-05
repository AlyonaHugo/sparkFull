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


    private static final String TASK_NAME = "Aggrigation task";
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
        
        JavaPairRDD<String, Integer> showAmountMap = showAmountRDD
                .mapToPair(splitStringInMapStringInteger());
        showAmountMap.collect().forEach(System.out::println);
        System.out.println("----------end of show amount map-----_----");
        
        JavaPairRDD<String, Integer> combined = findTotalAmountPerChannel(showChannelMap, showAmountMap);

        combined.collect()
                .forEach(System.out::println);
        System.out.println("*******************end********************");


        // Delete old file here
        deleteOldFile();

        combined.saveAsTextFile(RESULT_FILE);
        
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
    
    
    private static PairFunction<String, String, Integer> splitStringInMapStringInteger() {
        return new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                int show = 0;
                int amount = 1;
                String[] mas = s.split(",");
                return new Tuple2<String, Integer>(mas[show], new Integer(mas[amount]));
            }
        };
    }
    
    
    private static JavaPairRDD<String, Integer> findTotalAmountPerChannel(JavaPairRDD<String, String> showChannelMap,
                                                                          JavaPairRDD<String, Integer> collectionTypeWithSize) {
        return showChannelMap
                .join(collectionTypeWithSize)
                .mapToPair(createChannelAmountMap())
                .reduceByKey((a, b) -> a + b);
    }

    private static PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Integer> createChannelAmountMap() {
        return new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<String, Integer>> s) {
                return new Tuple2<String, Integer>(s._2._1.toString(), new Integer( s._2._2));
            }
        };
    }

    private static void deleteOldFile() {
        FileSystem hdfs = null;
        Path newFolderPath = null;
        try {
            hdfs = FileSystem.get(new Configuration());
            newFolderPath = new Path(RESULT_FILE);
            if(hdfs.exists(newFolderPath)){
                hdfs.delete(newFolderPath, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}


