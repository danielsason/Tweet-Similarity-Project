import Model.FinalTweetPresentation;
import Model.TweetInputFormat;
import Model.TweetKey;
import Model.TweetValue;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by Daniel on 6/3/2017.
 */
public class Phase2 {

    //value returned (Text) - words, the tweet value, separated by lines
    public static class Mapper2
            extends Mapper<TweetKey, TweetValue, FinalTweetPresentation, Text> {

        private Text text;
        HashMap<String, Boolean> stopWords;
        HashMap<String, Boolean> noDuplicatesWords;
        private final String REGEX = "[^a-zA-Z ]+";

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            text = new Text();
            stopWords = new HashMap<>();
            noDuplicatesWords = new HashMap<>();

            //populate stop words
            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);

            System.out.print("Downloading corpus description file from S3... ");
            S3Object object = s3.getObject(new GetObjectRequest("daniel-gabi-tweet-similarity", "stop-words.txt"));
            System.out.println("Done.");
            Scanner sc = new Scanner(new InputStreamReader(object.getObjectContent()));
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                line = line.replaceAll(REGEX, "");
                stopWords.put(line, true);
            }
            sc.close();
        }

        @Override
        public void map(TweetKey key, TweetValue value, Context context) throws IOException, InterruptedException {
            String tweetText = value.getText();
            String[] tweetWords = tweetText.toLowerCase().replaceAll(REGEX, "").split(" ");
            StringBuilder words = new StringBuilder();

            for (String word : tweetWords) {
                if (word.equals("") || stopWords.containsKey(word))         // || noDuplicatesWords.containsKey(word)
                    continue;

                words.append(word);
                words.append(" ");
            }

            //deleting last space if needed
            if (words.length() > 0)
                words.setLength(words.length() - 1);

            text.set(words.toString());

            FinalTweetPresentation newKey = new FinalTweetPresentation(
                    key.getCreatedAt(), key.getId(), value.getText(), value.isFavorited(), value.isRetweeted(), null);

            context.write(newKey, text);
        }
    }

    //key returned (Text) - FinalTweetPresentasio.toString(), our implemtent to use json objexts
    //value returned (Text) - vector as follow: word1atTweet$numoftweetspresent# word1atTweet$numoftweetspresent.....
    public static class Reducer2
            extends Reducer<FinalTweetPresentation, Text, Text, Text> {

        long N;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

                        //******* get from output of phase1 the total number of tweets *******
            String line;
            Path pt = new Path("hdfs:///output1/words.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

            while ((line = br.readLine()) != null) {
                String[] lineAfterSplit = line.split("\t");

                if (lineAfterSplit[0].equals("***numberOfTweets***")) {
                    this.N = Long.parseLong(lineAfterSplit[1]);
                    System.out.println("total number of tweets is : " + this.N);
                    break;
                }
            }
            br.close();
                                         //******* finished *******
        }

        @Override
        public void reduce(FinalTweetPresentation key, Iterable<Text> text, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> words_appearances = new HashMap<String, Integer>();
            HashMap<String,Long> word_numOfTweets = new HashMap<>();

            for (Text tweet : text) {
                String tweetText = tweet.toString();
                String[] words = tweetText.split(" ");
                Integer maxAppearances = 0;

                for (String word : words) {
                    if (!words_appearances.containsKey(word))
                        words_appearances.put(word, 1);
                    else
                        words_appearances.put(word, words_appearances.get(word) + 1);

                    if (words_appearances.get(word) > maxAppearances)
                        maxAppearances = words_appearances.get(word);
                }

                for (String word : words_appearances.keySet())
                    word_numOfTweets.put(word,(long)0);

                String line;
                Path pt = new Path("hdfs:///output1/words.txt");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

                while ((line = br.readLine()) != null) {
                    String[] lineAfterSplit = line.split("\t");

                    if (word_numOfTweets.containsKey(lineAfterSplit[0]))
                        word_numOfTweets.put(lineAfterSplit[0], Long.parseLong(lineAfterSplit[1]));
                }

                double tf, idf;
                Map<String,Object> vector = new HashMap<>();            //contains words vs tf_idf value

                for (String word : words_appearances.keySet()) {
                    Double tf_idf;

                    idf = Math.log(this.N / word_numOfTweets.get(word));
                    tf = 0.5 + (0.5 * words_appearances.get(word) / maxAppearances);
                    tf_idf = tf * idf;

                    vector.put(word, tf_idf);
                }


                JSONObject jsonVector = new JSONObject(vector);

                Text word_tfIdfText = new Text();
                word_tfIdfText.set(jsonVector.toString());

                key.setVector(vector);           //update finalTweet's vector
                Text newKey = new Text();
                newKey.set(key.toString());

                context.write(newKey, word_tfIdfText);
            }
        }
    }
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 2");
        job.setJarByClass(Phase2.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setInputFormatClass(TweetInputFormat.class);
        job.setMapOutputKeyClass(FinalTweetPresentation.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //job.getConfiguration().get("totalTweets");


        System.out.println("Phase 2 - input path: " + args[0] + ", output path: " + args[1]);
        try {
            if (job.waitForCompletion(true)) {
                System.out.println("Phase 2: job completed successfully");

                FileSystem fs = FileSystem.get(conf);               //merging output job2 distributed files
                Path srcPath = new Path(args[1]);

                Path dstPath = new Path("hdfs:///util/output2.txt");
                if(FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf, null))
                    System.out.println("phase 2: merge done successfully");
            }
            else
                System.out.println("Phase 2: job completed unsuccessfully");



        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

