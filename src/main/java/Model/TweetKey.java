package Model;


import org.apache.hadoop.io.WritableComparable;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Glazard18 on 6/2/2017.
 */
public class TweetKey implements WritableComparable<TweetKey> {
    String createdAt;
    Long id;

    public TweetKey(){
        createdAt = null;
        id = null;
    }

    public TweetKey(JSONObject tweetJson){
        createdAt = tweetJson.getString("created_at");
        id = tweetJson.getLong("id");
    }


    @Override
    public int compareTo(TweetKey other) {
        return (int) (id - other.id);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(createdAt);
        dataOutput.writeLong(id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        createdAt = dataInput.readUTF();
        id = dataInput.readLong();
    }

    public Long getId(){
        return this.id;
    }

    public String getCreatedAt() {
        return createdAt;
    }
}
