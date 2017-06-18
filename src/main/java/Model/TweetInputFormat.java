package Model;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by Glazard18 on 6/2/2017.
 */
public class TweetInputFormat extends FileInputFormat<TweetKey, TweetValue> {
    @Override
    public RecordReader<TweetKey, TweetValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TweetRecordReader();
    }
}
