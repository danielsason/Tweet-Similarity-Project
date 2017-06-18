import Model.TweetInputFormat;
import Model.TweetKey;
import Model.TweetValue;
import Model.WordTweedPair;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by Daniel on 6/2/2017.
 */
public class Phase1 {

    public static class Mapper1
            extends Mapper<TweetKey, TweetValue, Text, LongWritable>{

        //enum CounterEnum {Total_tweets};
        private final static LongWritable one = new LongWritable(1);
        private Text text;
        HashMap<String, Boolean> stopWords;            //hashmap for no duplicates words
        private final String REGEX = "[^a-zA-Z ]+";
        //private Counter tweetCounter;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            text = new Text();
            stopWords = new HashMap<>();

            //populate stop words
            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);

            //tweetCounter = context.getCounter(CounterEnum.class.getName(), CounterEnum.Total_tweets.toString());

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
            HashMap<String, Boolean> duplicates = new HashMap<>();
            String tweetText = value.getText();
            String[] tweetWords = tweetText.toLowerCase().replaceAll(REGEX, "").split(" ");
            for (String word : tweetWords){
                if (word.equals("") || stopWords.containsKey(word))
                    continue;

                if(!duplicates.containsKey(word)) {
                    text.set(word);
                    context.write(text, one);
                    duplicates.put(word,true);
                    //WordTweedPair pair = new WordTweedPair(word, key);
                    //context.write(pair, one);
                }
            }
            text.set("***numberOfTweets***");
            context.write(text, one);
            //tweetCounter.increment(1);
        }
    }

    public static class Reducer1
            extends Reducer<Text, LongWritable, Text, LongWritable>{

        @Override
        public void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(word, new LongWritable(sum));
        }
    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 1");
        job.setJarByClass(Phase1.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setInputFormatClass(TweetInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("Phase 1 - input path: " + args[0] + ", output path: " + args[1]);
        try {
            if (job.waitForCompletion(true)) {
                System.out.println("Phase 1: job completed successfully");

                FileSystem fs = FileSystem.get(conf);
                Path srcPath = new Path(args[1]);

                Path dstPath = new Path(args[1] + "words.txt");
                if(FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf, null))
                    System.out.println("phase 1: merge done successfully");
            }
            else
            System.out.println("Phase 1: job completed unsuccessfully");



            //Counter tweetCounter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.Task$Counter", "MAP_INPUT_RECORDS");
            //System.out.println("sum of tweets: " + tweetCounter.getValue());
//            Job job2 = Job.getInstance(conf, "Phase 2");
//            job2.getConfiguration().setLong("totalTweets", tweetCounter.getValue());



            //File file = new File("Total_Tweets.txt");
            //FileWriter fw = new FileWriter(file);

            //fw.write();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
