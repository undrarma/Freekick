import org.apache.spark.streaming.api.java.JavaDStream;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

public class PlayerSentiments {

    public static void storeSentimentsCount(String user) {
        Twitter twitter = new TwitterFactory().getInstance();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("src\\main\\resources\\input\\" + user + ".csv"))) {
            try {
                StringBuilder sb = new StringBuilder();
                List<Status> statuses;
                statuses = twitter.getUserTimeline(user);
                System.out.println("Showing @" + user + "'s user timeline.");
                SentimentAnalyzer.init();
                for (Status status : statuses) {
                    String elementRaw = status.getId() + ";" + status.getUser().getScreenName() + ";" + status.getText() + "\n";
                    sb.append(elementRaw);
                }
                bw.write(sb.toString());
                bw.close();
            } catch (TwitterException te) {
                te.printStackTrace();
                System.out.println("Failed to get timeline: " + te.getMessage());
                System.exit(-1);
            }
        }
        catch (Exception e){
            e.printStackTrace();
            System.out.println("Failed to write file: " + e.getMessage());
            System.exit(-1);
        }
    }
}
