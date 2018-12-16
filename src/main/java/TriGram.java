import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriGram {
    private static final String GoogleHebrew3Grams = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    private static final String myBucketName = "assignment2-hadoop";

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


        Configuration conf = new Configuration();
        Job job = new Job(conf, "hadoop-ass2");
        job.setJarByClass(TriGram.class); //TODO
        job.setMapperClass(TriGram.MapperClass.class);
        job.setPartitionerClass(TriGram.PartitionerClass.class); //TODO
        job.setReducerClass(TriGram.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(GoogleHebrew3Grams));
        FileOutputFormat.setOutputPath(job, new Path("https://" + myBucketName + ".s3.amazonaws.com/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }




}
