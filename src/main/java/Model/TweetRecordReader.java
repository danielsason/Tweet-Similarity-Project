package Model;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by Glazard18 on 6/2/2017.
 */
public class TweetRecordReader extends RecordReader<TweetKey,TweetValue> {

    LineRecordReader reader;

    public TweetRecordReader(){
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
    public TweetKey getCurrentKey() throws IOException, InterruptedException {
        return new TweetKey(new JSONObject(reader.getCurrentValue().toString()));
    }

    @Override
    public TweetValue getCurrentValue() throws IOException, InterruptedException {
        return new TweetValue(new JSONObject(reader.getCurrentValue().toString()));
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
