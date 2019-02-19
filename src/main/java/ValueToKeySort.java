import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

public class ValueToKeySort {

    public static class MapperClass extends Mapper<LongWritable, Text, PairTextDoubleWritable, Text> {

        private String[] words;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            words = value.toString().split("\t");
            DoubleWritable probability = new DoubleWritable(Double.valueOf(words[1]));
            String[] trigram = words[0].split(" ");
            if(trigram.length>2) {
                context.write(new PairTextDoubleWritable(new Text(trigram[0] + " " + trigram[1]),probability),
                        new Text(trigram[2]));
            }
        }
    }

    public static class ReducerClass extends Reducer<PairTextDoubleWritable,Text,Text,DoubleWritable> {

        @Override
        public void reduce(PairTextDoubleWritable key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for(Text w3 : values) {
                context.write(new Text(key.first.toString()+" "+w3.toString()),key.second);
            }
        }

    }





    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "G:/hadoop-2.6.2/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Sort");
        job.setJarByClass(ValueToKeySort.class);

        job.setMapOutputKeyClass(PairTextDoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setNumReduceTasks(1);

        int argsLength = args.length;
        FileInputFormat.addInputPath(job, new Path(args[argsLength-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[argsLength-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
