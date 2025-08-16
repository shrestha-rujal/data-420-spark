// Import classes from the java and hadoop packages

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Code is grouped in an outer class that can be executed

public class WordCount {

  // The problem specific map and reduce classes extend the generic hadoop Mapper and Reducer classes respectively

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    // We declare any variables that we need outside the map method

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // The map method is where we generate (key, value) pairs to be reduced

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // StringTokenizer itr = new StringTokenizer(value.toString());
      // while (itr.hasMoreTokens()) {
      //   word.set(itr.nextToken());
      //   context.write(word, one);
      // }

        // modified mapper to count unique characters
        String line = value.toString();

        for (char c : line.toCharArray()) {
            if (!Character.isWhitespace(c)) {
                word.set(Character.toString(c));
                context.write(word, one);
            }
        }
    }

  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // We declare any variables that we need outside the reduce method

    private IntWritable result = new IntWritable();

    // The reduce method is where we reduce an iterable of values which share a key

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }

  }

  // Execution starts in the main() method

  public static void main(String[] args) throws Exception {

    // We define a configuration object which retrieves default configuration from disk

    Configuration conf = new Configuration();

    // We create an instance of a Job and provide it with references to the main class, the mapper and combiner classes,
    // and output key and value classes so that hadoop knows how to load our input data, execute our code, and save the
    // output data consistently

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // We use helper classes from the hadoop package to manage loading the input and writing the output for the job

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // We start the job and wait for it to complete before exiting

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}