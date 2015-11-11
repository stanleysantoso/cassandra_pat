import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.log4j.varia.NullAppender;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.UUID;

/**
 * Created by Stanley on 11/11/2015.
 */
public class SimpleTweet {
    private static Cluster cluster;
    private static Session session;
    public static void main(String[] args) {
        org.apache.log4j.BasicConfigurator.configure(new NullAppender());
        // Connect to the cluster and keyspace "pat086"
        cluster = Cluster.builder().addContactPoint("167.205.35.20").build();
        session = cluster.connect("pat086");

        System.out.println("Insert command, use HELP for help");

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print(">");System.out.flush();
                String line=in.readLine();
                if(line.startsWith("quit") || line.startsWith("exit")){
                    break;
                }
                else if(line.startsWith("register")){
                    String username = line.substring(line.indexOf(" ")+1,line.indexOf(" ",line.indexOf(" ") + 1));
                    String password = line.substring(line.indexOf(" ",line.indexOf(" ") + 1)+1, line.length());
                    register(username,password);
                }
                else if(line.startsWith("follow")){
                    String username = line.substring(line.indexOf(" ")+1,line.indexOf(" ",line.indexOf(" ") + 1));
                    String friend = line.substring(line.indexOf(" ",line.indexOf(" ") + 1)+1, line.length());
                    followfriend(username,friend);
                }
                else if(line.startsWith("tweet")){
                    String username = line.substring(line.indexOf(" ")+1,line.indexOf(" ",line.indexOf(" ") + 1));
                    String tweet = line.substring(line.indexOf(" ",line.indexOf(" ") + 1)+1, line.length());
                    tweet(username, tweet);
                }
                else if(line.startsWith("showTweet")){
                    String username = line.substring(line.indexOf(" ")+1,line.length());
                    showUserline(username);
                }
                else if(line.startsWith("showTimeline")){
                    String username = line.substring(line.indexOf(" ")+1,line.length());
                    showTimeline(username);
                }
                else if(line.startsWith("HELP")){
                    System.out.print("List of commands : \n" +
                            "1. register (username) (password) \n" +
                            "2. follow (follower) (followee) \n" +
                            "3. tweet (username) (tweet) \n" +
                            "4. showTweet (username)\n" +
                            "5. showTimeline (username)\n");
                }
                else{
                    System.out.println("Command unknown !");
                }
            }
            catch(Exception e) {
            }
        }


        cluster.close();
    }

    public static void register(String username, String password){
        // Insert one record into the users table
        session.execute("INSERT INTO users (username, password) VALUES ('"+ username +"', '"+ password +"')");
    }

    public static void followfriend(String username, String friend){
        // Insert one record into the friends table
        session.execute("INSERT INTO friends (username, friend, since) VALUES ('"+ username +"', '"+ friend +"', dateof(now()))");
        // Insert one record into the followers table
        session.execute("INSERT INTO followers (username, follower, since) VALUES ('"+ friend +"', '"+ username +"', dateof(now()))");

    }

    public static void tweet(String username, String tweetbody){
        UUID tempUUID = UUID.randomUUID();
        // Insert tweet into the tweets table
        session.execute("INSERT INTO tweets (tweet_id, username, body) VALUES ("+tempUUID+", '"+ username +"', '"+ tweetbody + "')");

        // Insert tweet into the userline table
        session.execute("INSERT INTO userline (username, time, tweet_id) VALUES ('"+ username +"', now(), "+tempUUID+")");
        // Insert tweet into the timeline table
        session.execute("INSERT INTO timeline (username, time, tweet_id) VALUES ('"+ username +"', now(), "+tempUUID+")");
        // Insert tweet to all followers timeline
        ResultSet results = session.execute("SELECT * FROM followers where username = '"+ username +"'");
        for (Row row : results) {
            session.execute("INSERT INTO timeline (username, time, tweet_id) VALUES ('" + row.getString("follower")+"', now(), "+tempUUID+")");
        }
    }

    public static void showUserline(String username){
        ResultSet results = session.execute("SELECT * FROM userline where username = '"+ username +"'");
        for (Row row : results) {
            ResultSet tempResults  = session.execute("SELECT * FROM tweets where tweet_id = "+row.getUUID("tweet_id"));
            Row tempTweet = tempResults.all().get(0);
            System.out.format("%s tweets \"%s\" \n", tempTweet.getString("username"), tempTweet.getString("body"));
        }
    }

    public static void showTimeline(String username){
        ResultSet results = session.execute("SELECT * FROM timeline where username = '"+ username +"'");
        for (Row row : results) {
            ResultSet tempResults  = session.execute("SELECT * FROM tweets where tweet_id = "+row.getUUID("tweet_id"));
            Row tempTweet = tempResults.all().get(0);
            System.out.format("%s tweets \"%s\" \n", tempTweet.getString("username"), tempTweet.getString("body"));
        }
    }

}
