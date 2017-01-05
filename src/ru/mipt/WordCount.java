package ru.mipt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;


public class WordCount extends Configured implements Tool {

  public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
      for(String element: line.toString().split("\\s+")) {
        element = element.replaceAll("[^A-Za-z]+", "");
        if (element.length() >= 6 && element.length() <= 9) {
          word.set(element);
          context.write(word, one);
        }
      }
    }
  }

  public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable count = new IntWritable();
    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<IntWritable> it = values.iterator();
      int sum = 0;
      while (it.hasNext()){
        sum += it.next().get();
      }
      count.set(sum);
      context.write(word, count);
    }
  }

  public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text > {
    private Text temp = new Text();
    public void reduce(IntWritable count, Iterable<Text> words, Context context) throws IOException, InterruptedException {
      Iterator<Text> it = words.iterator();
      while(it.hasNext())
        context.write(count, it.next());
    }
  }

  public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable count = new IntWritable();
    private Text word = new Text();
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
      String[] pair = line.toString().split("\\s+");
      word.set(pair[0]);
      count.set(Integer.parseInt(pair[1]));
      context.write(count, word);
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Path inputPath = new Path(strings[0]);
    Path tempPath = new Path(strings[1]);
    Path outputPath = new Path(strings[2]);

    Job counterJob = Job.getInstance();

    counterJob.setJarByClass(WordCount.class);
    counterJob.setMapperClass(WordMapper.class);
    counterJob.setReducerClass(CountReducer.class);
    counterJob.setOutputKeyClass(Text.class);
    counterJob.setOutputValueClass(IntWritable.class);
    counterJob.setInputFormatClass(TextInputFormat.class);
    counterJob.setOutputFormatClass(TextOutputFormat.class);
    counterJob.setMapOutputKeyClass(Text.class);
    counterJob.setMapOutputValueClass(IntWritable.class);
    counterJob.setNumReduceTasks(8);

    TextInputFormat.addInputPath(counterJob, inputPath);
    TextOutputFormat.setOutputPath(counterJob, tempPath);

    counterJob.waitForCompletion(true);

    Job sortingJob = Job.getInstance();

    sortingJob.setJarByClass(WordCount.class);
    sortingJob.setMapperClass(SortMapper.class);
    sortingJob.setReducerClass(SortReducer.class);
    sortingJob.setOutputKeyClass(IntWritable.class);
    sortingJob.setOutputValueClass(Text.class);
    sortingJob.setInputFormatClass(TextInputFormat.class);
    sortingJob.setOutputFormatClass(TextOutputFormat.class);
    sortingJob.setMapOutputKeyClass(IntWritable.class);
    sortingJob.setMapOutputValueClass(Text.class);
    sortingJob.setNumReduceTasks(1);

    TextInputFormat.addInputPath(sortingJob, tempPath);
    TextOutputFormat.setOutputPath(sortingJob, outputPath);

    return sortingJob.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    new WordCount().run(args);
  }
}
