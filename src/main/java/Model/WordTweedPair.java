package Model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Glazard18 on 6/2/2017.
 */
public class WordTweedPair implements WritableComparable<WordTweedPair> {

    String word;
    TweetKey tweetKey;

    public WordTweedPair(String word, TweetKey tweetKey){
        this.word = word;
        this.tweetKey = tweetKey;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        tweetKey.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readUTF();
        tweetKey.readFields(dataInput);
    }

    @Override
    public int compareTo(WordTweedPair other) {
        return tweetKey.compareTo(other.tweetKey);
    }

    public TweetKey getTweetKey(){
        return this.tweetKey;
    }
}
