
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

public class TriGrams {
    public enum N{
        Count
    };

    public static class MapperClass extends Mapper<LongWritable, Text, Text, PairIntIntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text ngrams = new Text("");
        private String[] words;
        private String line;



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            line = value.toString();
            words = line.split("\t");
            if(words.length<1)
                return;
            ngrams.set(words[0]);
            String[] ngram = words[0].split(" ");
            if(ngram.length < 3)
                return;
            int group = (int)(Math.random() * 2);

            //Counting number of ngrams
            context.getCounter(N.Count).increment(1);

            PairIntIntWritable groupToCount = new PairIntIntWritable(one, zero);
            if(group == 1)
                groupToCount = new PairIntIntWritable(zero, one);
            context.write(ngrams, groupToCount);
        }



    }

    public static class ReducerClass extends Reducer<Text,PairIntIntWritable,Text,PairIntIntWritable> {


        @Override
        public void reduce(Text key, Iterable<PairIntIntWritable> values, Context context) throws IOException,  InterruptedException {
            int sumGroupA = 0;
            int sumGroupB = 0;
            for (PairIntIntWritable value : values) {
                    sumGroupA += value.first.get();
                    sumGroupB += value.second.get();
            }
            context.write(key, new PairIntIntWritable(new IntWritable(sumGroupA), new IntWritable(sumGroupB)));        }
    }





    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "G:/hadoop-2.6.2/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        int argsLength = args.length;

        String myBucketname = args[argsLength-3];
        conf.set("bucketname", myBucketname);

        Job job = new Job(conf, "TriGramsCount");
        job.setJarByClass(TriGrams.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairIntIntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairIntIntWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        //job.setCombinerClass(ReducerClass.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[argsLength-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[argsLength-1]));

        boolean end = job.waitForCompletion(true);
        writeN(conf, myBucketname, job);
        System.exit(end ? 0 : 1);



    }

    public static void writeN(Configuration conf, String bucketname, Job job)  throws IOException, InterruptedException {
        long n = job.getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketname), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + bucketname + "/N.txt"));
        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
        writer.write(String.valueOf(n));
        writer.close();
        fsDataOutputStream.close();

    }



}
