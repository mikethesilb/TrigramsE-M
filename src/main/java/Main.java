import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Main {


    private static final String myBucketName = "assignment2-mikethesilb";
    private static final String mapReduceFilename = "ass2.jar";
    private static final String myKeyPair = "mike_keypair";


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider);



        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://" + myBucketName + "/" + mapReduceFilename + ".jar") // This should be a full map reduce application.
                .withMainClass("TriGrams")
                .withArgs("s3n://" + myBucketName + "/input/", "s3n://" + myBucketName + "/output/");

        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6)
                .withMasterInstanceType(InstanceType.T2Micro.toString())
                .withSlaveInstanceType(InstanceType.T2Micro.toString())
                .withHadoopVersion("2.8.0").withEc2KeyName(myKeyPair)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1b"));


        ///////////////////////////////////////////////////////////




        //////////////////////////////////////////////////////////
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("hadoop-ass2")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withReleaseLabel("emr-4.1.0")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3://" + myBucketName + "/");


        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
