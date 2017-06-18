import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.io.Text;

import java.util.HashMap;

/**
 * Created by Glazard18 on 6/3/2017.
 */
public class TweetSimilarity {

    public static void main(String[] args) {
        PropertiesCredentials credentials;
        try {
            credentials = new PropertiesCredentials(
                    TweetSimilarity.class.getResourceAsStream("creds.properties"));
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. ", e);
        }

        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

        HadoopJarStepConfig jarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://daniel-gabi-tweet-similarity/assignmnet2/tweet-similarity.jar")
                .withMainClass("Phase1")
                .withArgs("s3://daniel-gabi-tweet-similarity/assignmnet2/TinyCorpus.txt", "hdfs:///output1/");

        StepConfig step1Config = new StepConfig()
                .withName("Phase 1")
                .withHadoopJarStep(jarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://daniel-gabi-tweet-similarity/assignmnet2/tweet-similarity.jar")
                .withMainClass("Phase2")
                .withArgs("s3://daniel-gabi-tweet-similarity/assignmnet2/TinyCorpus.txt", "hdfs:///output2/");

        StepConfig step2Config = new StepConfig()
                .withName("Phase 2")
                .withHadoopJarStep(jarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://daniel-gabi-tweet-similarity/assignmnet2/tweet-similarity.jar")
                .withMainClass("Phase3")
                .withArgs("hdfs:///output2/", "hfds:///output3/", args[0]);

        StepConfig step3Config = new StepConfig()
                .withName("Phase 3")
                .withHadoopJarStep(jarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(10)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.7.3")
                .withEc2KeyName("danielsason")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("extract-related-word-pairs")
                .withInstances(instances)
                .withSteps(step1Config, step2Config, step3Config)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withReleaseLabel("emr-5.6.0")
                .withLogUri("s3n://daniel-gabi-tweet-similarity/assignmnet2/logs/");

        System.out.println("Submitting the JobFlow Request to Amazon EMR and running it...");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

//        String str = "aaa bbb";
//        String[] arr = str.split(" ");
//        StringBuilder bigString = new StringBuilder();
//
//        for(String stri : arr) {
//            bigString.append(stri);
//            bigString.append(" ");
//        }
//        if (bigString.length() > 0) {
//            bigString.setLength(bigString.length() - 1);
//        }
//
//        Text text = new Text();
//        str = bigString.toString();
//        text.set(str);
//        //text.set(arr[1]);
//        System.out.println(text.toString() + ".");
//
//        HashMap<String, Integer> words_appearances = new HashMap<String, Integer>();
//        words_appearances.put("aa", new Integer(1));
//        words_appearances.put("aa", words_appearances.get("aa") + 1);
//
//        System.out.println(words_appearances.get("aa"));
//
//        Integer a = new Integer(0);
//        Integer b = new Integer(1);
//
//        System.out.println(a);
//
//        if(a<b)
//            a=b;
//
//        System.out.println(a);
    }
}
