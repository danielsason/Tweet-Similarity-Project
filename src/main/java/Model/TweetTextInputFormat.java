package Model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by Daniel on 16/06/2017.
 */
public class TweetTextInputFormat extends FileInputFormat<FinalTweetPresentation, Text>{
    @Override
    public RecordReader<FinalTweetPresentation, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TweetTextRecordReader();
    }
}
