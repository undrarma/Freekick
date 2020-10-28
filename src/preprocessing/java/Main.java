import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

import java.io.BufferedReader;
import java.io.FileReader;


public class Main {

    static String TWITTER_CONFIG_PATH = "src/main/resources/twitter_configuration.txt";
    static String HADOOP_COMMON_PATH = "C:\\Users\\a0081\\Documents\\BDMA\\UPC\\SDM\\Lab 02\\SparkGraphXassignment\\src\\main\\resources";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        SparkConf conf = new SparkConf().setAppName("SparkStreamingTraining").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        Utils.setupTwitter(TWITTER_CONFIG_PATH);
//        try (BufferedReader playersCSV = new BufferedReader(new FileReader(args[0]))) {
//            String line;
//            while ((line = playersCSV.readLine()) != null) {
//                String user = line;
//                PlayerSentiments.storeSentimentsCount(user);
//            }
//        }
//        TwitterTransformation.transformTweets(ctx);
//        jsc.start();
//        jsc.awaitTermination();
//        createURIs.storeSentimentsCount();
        twitterABOX.generateTwitterABOX();
    }
}
