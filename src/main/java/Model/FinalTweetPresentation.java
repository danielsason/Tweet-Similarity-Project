package Model;

import org.apache.hadoop.io.WritableComparable;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Daniel on 17/06/2017.
 */
public class FinalTweetPresentation implements WritableComparable<FinalTweetPresentation> {

    Long id;
    String createdAt;
    String text;
    boolean favorited;
    boolean retweeted;
    Map<String, Object> vector;         // contains words and tf_idf value

    public FinalTweetPresentation() {
        id = (long) -1;
        createdAt = null;
        text = null;
        favorited = false;
        retweeted = false;
        vector = null;
    }

    public FinalTweetPresentation(String createdAt, Long id, String text, boolean favorited, boolean retweeted, HashMap vector) {
        this.id = id;
        this.createdAt = createdAt;
        this.text = text;
        this.favorited = favorited;
        this.retweeted = retweeted;
        this.vector = vector;
    }

    public FinalTweetPresentation(JSONObject tweetJson) {
        id = tweetJson.getLong("id");
        createdAt = tweetJson.getString("created_at");
        text = tweetJson.getString("text");
        favorited = tweetJson.getBoolean("favorited");
        retweeted = tweetJson.getBoolean("retweeted");
        try {
            JSONObject json = tweetJson.getJSONObject("vector");
            vector = json.toMap();
        }catch (Exception e){
            vector =null;
        }
    }

    @Override
    public int compareTo(FinalTweetPresentation o) {
        return (int) (id - o.id);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(createdAt);
        dataOutput.writeUTF(text);
        dataOutput.writeBoolean(favorited);
        dataOutput.writeBoolean(retweeted);
        JSONObject obj = new JSONObject(vector);
        dataOutput.writeUTF(obj.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        createdAt = dataInput.readUTF();
        text = dataInput.readUTF();
        favorited = dataInput.readBoolean();
        retweeted = dataInput.readBoolean();
        JSONObject obj = new JSONObject(dataInput.readUTF());
        vector = obj.toMap();
    }

    public Long getId() {
        return id;
    }

    public void setVector(Map vector) {
        this.vector = vector;
    }

    public Map<String, Object> getVector() {
        return vector;
    }

    @Override
    public String toString() {
        JSONObject json = new JSONObject();

        json.put("created_at", this.createdAt);
        json.put("id", this.id);
        json.put("text", this.text);
        json.put("favorited", this.favorited);
        json.put("retweeted", this.retweeted);
        json.put("vector", this.vector);

        return json.toString();
    }
}
