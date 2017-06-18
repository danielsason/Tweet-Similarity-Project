package Model;

import org.json.JSONObject;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Glazard18 on 6/2/2017.
 */
public class TweetValue implements Writable {

    String userName;
    String text;
    boolean favorited;
    boolean retweeted;

    public TweetValue(){
        userName = null;
        text = null;
        favorited = false;
        retweeted = false;
    }


    public TweetValue(JSONObject tweetJson){
        userName = tweetJson.getJSONObject("user").getString("name");
        text = tweetJson.getString("text");
        favorited = tweetJson.getBoolean("favorited");
        retweeted = tweetJson.getBoolean("retweeted");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(text);
        dataOutput.writeBoolean(favorited);
        dataOutput.writeBoolean(retweeted);
    }

    public String getUserName() {
        return userName;
    }

    public boolean isFavorited() {
        return favorited;
    }

    public boolean isRetweeted() {
        return retweeted;
    }

    @Override

    public void readFields(DataInput dataInput) throws IOException {
        userName = dataInput.readUTF();
        text = dataInput.readUTF();
        favorited = dataInput.readBoolean();
        retweeted = dataInput.readBoolean();
    }

    public String getText(){
        return this.text;
    }
}
