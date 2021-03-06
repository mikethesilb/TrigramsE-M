import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CrossValidationCount {

    public static class MapperClass extends Mapper<LongWritable, Text,
            PairIntIntWritable, PairIntTextWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private String line;
        private String[] words;
        private Text ngrams = new Text("");
        private IntWritable firstCount = new IntWritable(0);
        private IntWritable secondCount = new IntWritable(0);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            line = value.toString();
            words = line.split("\t");
            if(words[0] != null)
                ngrams.set(words[0]);
            if(words[1] != null)
                firstCount.set(Integer.valueOf(words[1]));
            if(words[2] != null)
                secondCount.set(Integer.valueOf(words[2]));
            //first group count (the index of the group is 0), the value is the 3gram with the second group count
            context.write(new PairIntIntWritable(firstCount, zero), new PairIntTextWritable(secondCount, ngrams));
            //second group count (the index of the group is 1), the value is the 3gram with the first group count
            context.write(new PairIntIntWritable(secondCount, one), new PairIntTextWritable(firstCount, ngrams));
        }


    }

    public static class ReducerClass extends Reducer<PairIntIntWritable, PairIntTextWritable,
            Text,PairIntTextWritable> {

        @Override
        public void reduce(PairIntIntWritable key, Iterable<PairIntTextWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            String tag;
            List<String> ngrams = new LinkedList<>();
            for (PairIntTextWritable pair : values) {
                if(!(ngrams.contains(pair.second.toString()))){
                    ngrams.add(pair.second().toString());
                }
                sum += pair.first.get();
            }



            if(key.second.get() == 0) {
                tag = "TR01";
            } else {
                tag = "TR10";
            }

            for (String ngram : ngrams) {
                context.write(new Text(ngram), new PairIntTextWritable(new IntWritable(sum), new Text(tag)));
            }
        }

    }





    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "CrossValidation");
        job.setJarByClass(CrossValidationCount.class);

        job.setMapOutputKeyClass(PairIntIntWritable.class);
        job.setMapOutputValueClass(PairIntTextWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairIntTextWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setNumReduceTasks(1);

        int argsLength = args.length;
        FileInputFormat.addInputPath(job, new Path(args[argsLength-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[argsLength-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }




}
