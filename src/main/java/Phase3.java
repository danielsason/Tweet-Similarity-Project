import Model.FinalTweetPresentation;
import Model.TweetKey;
import Model.TweetTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Glazard18 on 6/16/2017.
 */
public class Phase3 {

    //key recieved (Text) - FinalTweetPresentasio
    //value recieved (Text) - vector as follow: word1atTweet$numoftweetspresent# word1atTweet$numoftweetspresent.....
    //returned (key, value)  same args as recieved - all related tweetes to finaltweet (tweets with common words) if doesnt related than doesnt sent
    public static class Mapper3
            extends Mapper<FinalTweetPresentation, Text, FinalTweetPresentation, Text> {

        @Override
        public void map(FinalTweetPresentation key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, Object> myVector = key.getVector();

            String line;
            Path pt = new Path("hdfs:///util/output2.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

            while ((line = br.readLine()) != null) {

                String[] lineAfterFirstSplit = line.split("\t");
                JSONObject otherVectorAsJson = new JSONObject(lineAfterFirstSplit[1]);      //take vector from other tweet
                JSONObject otherTweetAsJson = new JSONObject(lineAfterFirstSplit[0]);      //take finaltweet from other tweet
                Map<String, Object> otherVector = otherVectorAsJson.toMap();

                if (otherTweetAsJson.getLong("id") == key.getId())
                    continue;

                for (String word : myVector.keySet())
                    if (otherVector.containsKey(word)) {
                        JSONObject json = new JSONObject();
                        json.put("vector", otherVector);
                        json.put("text", otherTweetAsJson.getString("text"));
                        json.put("id", otherTweetAsJson.getLong("id"));

                        Text text = new Text();
                        text.set(json.toString());
                        context.write(key, text);
                        break;
                    }
            }
        }
    }

    public static class Reducer3
            extends Reducer<FinalTweetPresentation, Text, FinalTweetPresentation, Text> {

        int N;

        public void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getInt("N", 0);
        }

        @Override
        public void reduce(FinalTweetPresentation key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, Object> myVector = key.getVector();
            Map<Long, JSONObject> similarN = new HashMap<>();

            for (Text currTweet : values) {
                JSONObject otherTweetAsJson = new JSONObject(currTweet.toString());
                Map<String, Object> otherVector = otherTweetAsJson.getJSONObject("vector").toMap();

                double cosine_similarity, cosine_mon = 0, mySumftIdf = 0, otherSumTfIdf = 0;

                for (String currWord : myVector.keySet()) {
                    if (otherVector.containsKey(currWord)) {
                        cosine_mon += (double) otherVector.get(currWord) + (double) myVector.get(currWord);

                        mySumftIdf += Math.pow((double) myVector.get(currWord), 2);
                        otherSumTfIdf += Math.pow((double) otherVector.get(currWord), 2);
                    }
                }

                cosine_similarity = cosine_mon / ((Math.sqrt(mySumftIdf) * Math.sqrt(otherSumTfIdf)));

                if (similarN.size() < N) {
                    JSONObject text_cos_json = new JSONObject();
                    text_cos_json.put("text", otherTweetAsJson.getString("text"));
                    text_cos_json.put("cosine", cosine_similarity);
                    similarN.put(otherTweetAsJson.getLong("id"), text_cos_json);
                }
                else {
                    for (Long currId : similarN.keySet()) {
                        if (similarN.get(currId).getDouble("cosine") < cosine_similarity) {
                            similarN.remove(currId);
                            JSONObject text_cos_json = new JSONObject();
                            text_cos_json.put("text", otherTweetAsJson.getString("text"));
                            text_cos_json.put("cosine", cosine_similarity);
                            similarN.put(otherTweetAsJson.getLong("id"), text_cos_json);
                            break;
                        }
                    }
                }
            }

            //JSONObject json = new JSONObject();
            StringBuilder sb = new StringBuilder();

            for(Long currId: similarN.keySet()){
                sb.append(similarN.get(currId).getString("text") + "\t");
                sb.append(similarN.get(currId).getDouble("cosine") + "\n");
            }

            Text text = new Text();
            text.set(sb.toString());

            context.write(key, text);
        }
    }
//            int sum = 0;
//
//            for (Text value : values){
//                sum++;
//            }
//            context.write(key, new LongWritable(sum));


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 3");
        job.setJarByClass(Phase3.class);
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);
        job.setInputFormatClass(TweetTextInputFormat.class);
        job.setMapOutputKeyClass(FinalTweetPresentation.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(FinalTweetPresentation.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        conf.setInt("N", Integer.parseInt(args[2]));

        System.out.println("Phase 3 - input path: " + args[0] + ", output path: " + args[1]);
        try {
            if (job.waitForCompletion(true)) {
                System.out.println("Phase 3: job completed successfully");

                FileSystem fs = FileSystem.get(conf);               //merging output job2 distributed files
                Path srcPath = new Path(args[1]);

                Path dstPath = new Path("s3://daniel-gabi-tweet-similarity/assignmnet2/output3/");
                if(FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf, null))
                    System.out.println("phase 2: merge done successfully");

            } else
                System.out.println("Phase 3: job completed unsuccessfully");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
