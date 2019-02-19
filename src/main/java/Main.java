import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


public class Main {

    private static final String myBucketName = "assignment2-hadoop";//"assignment2-hadoop";
    private static final String TriGramsCount = "TriGrams";
    private static final String RAndCrossValCounts = "RAndCrossValCounts";
    //private static final String rCount = "RCount";
    //private static final String crossValidationCount = "CrossValCount";
    private static final String joinAndCalc = "JoinAndCalc";
    private static final String sortGrams = "Sort";
    private static final String myKeyPair = "eliortapiro";


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);



        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentialsProvider);


        List<StepConfig> steps = new LinkedList<StepConfig>();

        steps.add(createStepConfig(myBucketName + " input3grams", "output1", TriGramsCount, "TriGrams"));

        //======RCount and Cross Validation in 2 steps=====================//
        //steps.add(createStepConfig("output1", "output2", rCount, "RCount"));
        //steps.add(createStepConfig("output1", "output3", crossValidationCount, "CrossValidationCount"));
        //steps.add(createStepConfig(myBucketName + " output2 output3", "output4", joinAndCalc, "JoinAndCalculate"));

        //======RCount and Cross Validation in 1 step=====================//
        steps.add(createStepConfig("output1", "output2", RAndCrossValCounts, "RAndCrossValCounts"));
        steps.add(createStepConfig(myBucketName + " output2", "output3", joinAndCalc, "JoinAndCalculate"));
        //================================================================//


        steps.add(createStepConfig("output3", "result", sortGrams, "ValueToKeySort"));

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(4)
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
                .withSteps(steps)
                .withLogUri("s3://" + myBucketName + "/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }




    private static StepConfig createStepConfig(String input, String output, String jarName, String mainClass){
        String[] inputArgs = input.split(" ");
        int i=0;
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig();
        if(inputArgs.length == 2){
            hadoopJarStep.withArgs(inputArgs[0]);
            i++;
        }
        if(inputArgs.length == 3){
            hadoopJarStep.withArgs(inputArgs[0],
                    "s3n://" + myBucketName + "/" + inputArgs[1] + "/");
            i = i + 2;
        }
        hadoopJarStep.withJar("s3://" + myBucketName + "/" + jarName + ".jar") // This should be a full map reduce application.
                .withMainClass(mainClass) //TriGrams
                .withArgs("s3n://" + myBucketName + "/" + inputArgs[i] + "/", "s3n://" + myBucketName + "/" + output + "/");



        StepConfig stepConfig = new StepConfig()
                .withName("hadoop-" + jarName)
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        return stepConfig;

    }


}
