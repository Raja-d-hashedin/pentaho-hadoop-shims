package org.pentaho.hadoop.shim.common;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

public class WordCount2 {
    public static void submit(Job job1) {
        try {
            System.out.println("________________________ Wordcount2.submit(Job job) mtd _______________________________");
            System.out.println("######################## WordCount2.submit(Job job) ###################################");
           final Configuration conf = new Configuration();
           final Job job = Job.getInstance();
            job.setJarByClass((Class) WordCount2.class);
            job.setOutputKeyClass((Class) Text.class);
            job.setOutputValueClass((Class) IntWritable.class);
            job.setMapperClass((Class) Map.class);
            job.setReducerClass((Class) Reduce.class);
            job.setInputFormatClass((Class) TextInputFormat.class);
            job.setOutputFormatClass((Class) TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path("/user/mapr/input"));
            FileOutputFormat.setOutputPath(job, new Path("/user/mapr/wc1"));
            job.submit();
            System.out.println("######################## WordCount2.submit() Completed ###################################");
        }catch(Exception e)
        {
            System.out.println("######################## WordCount2.submit() Exception ###################################");
            e.printStackTrace();
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private static final IntWritable one;
        private Text word;

        static {
            one = new IntWritable(1);
        }

        public Map() {
            this.word = new Text();
        }

        public void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                this.word.set(tokenizer.nextToken());
                context.write((Object)this.word, (Object)Map.one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(final Text key, final Iterable<IntWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write((Object)key, (Object)new IntWritable(sum));
        }
    }
}
