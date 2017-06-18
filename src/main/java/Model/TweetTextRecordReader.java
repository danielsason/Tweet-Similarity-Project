package Model;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by Daniel on 16/06/2017.
 */
public class TweetTextRecordReader extends RecordReader<FinalTweetPresentation,Text> {

    LineRecordReader reader;

    public TweetTextRecordReader(){
        reader = new LineRecordReader();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public FinalTweetPresentation getCurrentKey() throws IOException, InterruptedException {
        return new FinalTweetPresentation(new JSONObject(reader.getCurrentValue().toString()));
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(reader.getCurrentValue().toString());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
