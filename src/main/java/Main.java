import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;


public class Main {

    private static final String myBuckeyname = "assignment2-hadoop";
    private static final String mapReduceFilename = "ass2";
    private static final String myKeyPair = "eliortapiro";


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
       String log4jConfPath = "G:/hadoop-2.6.2/etc/hadoop/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);



        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider);//AmazonElasticMapReduceClientBuilder.standard().withCredentials(credentialsProvider).build();


        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://" + myBuckeyname + "/" + mapReduceFilename + ".jar") // This should be a full map reduce application.
                .withMainClass("WordCount")
                .withArgs("s3n://" + myBuckeyname + "/input/", "s3n://" + myBuckeyname + "/output/");

        StepConfig stepConfig = new StepConfig()
                .withName("eliortapirohadoopstep")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.6.2").withEc2KeyName(myKeyPair)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));


        ///////////////////////////////////////////////////////////


        //////////////////////////////////////////////////////////
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withReleaseLabel("emr-4.2.0")
                //.withAmiVersion("")
                .withName("hadoop-ass2")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3://" + myBuckeyname + "/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
