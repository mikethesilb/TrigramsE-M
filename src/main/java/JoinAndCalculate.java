import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.io.IOException;
import java.net.URI;

public class JoinAndCalculate {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, PairIntTextWritable> {
        private final static IntWritable one = new IntWritable(1);
        private String[] words;
        private Text ngrams = new Text("");



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            words = value.toString().split("\t");
            ngrams.set(words[0]);
            PairIntTextWritable typeAndCount = new PairIntTextWritable(new IntWritable(Integer.valueOf(words[1])), new Text(words[2]));
            context.write(ngrams, typeAndCount);
        }




    }

    public static class ReducerClass extends Reducer<Text,PairIntTextWritable,Text,DoubleWritable> {

        private static Long N;
        private String bucket;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            String input;
            bucket = context.getConfiguration().get("bucketname");
            FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucket), context.getConfiguration());
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(("s3://" + bucket +"/N.txt")));//"C:\\Users\\elior\\eclipse-workspace\\DSP2\\eliortapirobucket\\N.txt"));//
            input = IOUtils.toString(fsDataInputStream, "UTF-8");
            fsDataInputStream.close();
            N = Long.valueOf(input);
        }

        @Override
        public void reduce(Text key, Iterable<PairIntTextWritable> values, Context context) throws IOException,  InterruptedException {
            Integer NR0 = null;
            Integer NR1 = null;
            Integer TR01 = null;
            Integer TR10 = null;
            for(PairIntTextWritable pair : values) {
                String tag = pair.second.toString();
                switch(tag) {
                    case("NR0"): {
                        NR0 = new Integer(pair.first.get());
                    }
                    case("NR1"): {
                        NR1 = new Integer(pair.first.get());
                    }
                    case("TR01"): {
                        TR01 = new Integer(pair.first.get());
                    }
                    case("TR10"): {
                        TR10 = new Integer(pair.first.get());
                    }
                }
            }
            if(NR0 == null || NR1 == null || TR01 == null || TR10 == null) {
                throw new IOException("NR0, NR1, TR01, TR10 not initialized properly.");
            }
            double probability = (TR01.doubleValue() + TR10.doubleValue())/(N.doubleValue()*(NR0.doubleValue()+NR1.doubleValue()));
            context.write(key,new DoubleWritable(probability));
        }

    }




    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        //String log4jConfPath = "C:/hadoop-2.8.0/etc/hadoop/log4j.properties";
        //PropertyConfigurator.configure(log4jConfPath);

        Configuration conf = new Configuration();
        int argsLength = args.length;
        String myBucketname = args[argsLength - 3];
        conf.set("bucketname", myBucketname);

        Job job = new Job(conf, "Join");
        job.setJarByClass(JoinAndCalculate.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairIntTextWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setNumReduceTasks(1);


        FileInputFormat.addInputPath(job, new Path(args[argsLength-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[argsLength-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }




}
