import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;

public class TriGrams {
    private static final String GoogleHebrew3Grams = "s3.amazonaws.com/datasets.elasticmapreduce/ngrams/books/20090715/heb-all/5gram/data";
    private static final String myBuckeyname = "assignment2-hadoop";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int i=1;
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(i!=1) {
                    context.write(word, one);
                }
                i++;
                if (i>3)
                    break;

            }
        }



    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {

        public int getPartition(Text key, IntWritable value, int numPartitions) {
              String s =  key.toString();
              return s.charAt(0);

        }
    }





    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);


        Configuration conf = new Configuration();
        Job job = new Job(conf, "trigrams-ass2");
        job.setJarByClass(TriGrams.class); //TODO
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MapperClass.class);
        //job.setPartitionerClass(PartitionerClass.class); //TODO
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }




}
