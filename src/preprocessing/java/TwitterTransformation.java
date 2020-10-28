import java.io.File;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class TwitterTransformation {
    public static void transformTweets(JavaSparkContext ctx) {
        String out = "";
        SentimentAnalyzer.init();
        SparkSession session = SparkSession.builder().master("local").appName("tweetScoring").getOrCreate();
        File dir = new File("src\\main\\resources\\input");
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                JavaRDD<String> tweets = ctx.textFile(child.getPath());
                List<Tuple2<String, Integer>> tweetSentiment = tweets.filter(t -> (t != null && t.length() > 0 && t != "\n\n"))
                        .filter(t -> Character.isDigit(t.charAt(0)))
                        .mapToPair(t -> new Tuple2<String, String>(t.split(";")[1], t.split(";")[2]))
                        .mapValues(t -> SentimentAnalyzer.cleanTweet(t))
                        .mapValues(t -> SentimentAnalyzer.findSentiment(t))
                        .aggregateByKey(new Tuple2<Double, Integer>(0.0, 0),
                                (sumCount, value) -> new Tuple2<Double, Integer>(sumCount._1 + value, sumCount._2 + 1),
                                (sumCount, value) -> new Tuple2<Double, Integer>(sumCount._1 + value._1, sumCount._2 + value._2))
                        .map(t -> new Tuple2<String, Integer>(String.valueOf(t._1), (int)(Math.round(t._2._1 / t._2._2))))
                        .collect();

                for (Tuple2<String,Integer> keyValue : tweetSentiment) {
                    out += keyValue._1 + ":" + keyValue._2 + " \n";
                }
                System.out.println(out);

                Dataset<Row> playerScore = session.createDataset(tweetSentiment, Encoders.tuple(Encoders.STRING(), Encoders.INT())).toDF("twitterAcc", "sentimentScore");
                playerScore.show();

                playerScore.write()
                        .format("jdbc")
                        .mode("append")
                        .option("url", "jdbc:postgresql://localhost:5432/Freekick")
                        .option("dbtable", "info.twitter")
                        .option("user", "postgres")
                        .option("password", "admin")
                        .save();
            }
        }
    }
}
