package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Stripes extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {
    
    Configuration conf = this.getConf();
      Job job = new Job(conf, "Stripes");

      job.setInputFormatClass(TextInputFormat.class);
      job.setMapperClass(StripesMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(StringToIntMapWritable.class);

      job.setReducerClass(StripesReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(StringToIntMapWritable.class);

      job.setOutputFormatClass(TextOutputFormat.class);

      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[1])); //Why?? from different lib, what's the difference
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      job.setNumReduceTasks(Integer.parseInt(args[0]));

      job.setJarByClass(Stripes.class);
      return job.waitForCompletion(true) ? 0 : 1;

  }

  public Stripes (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
    System.exit(res);
  }
}

class StripesMapper
extends Mapper<LongWritable, Text, Text, StringToIntMapWritable> { // TODO: change Object to output value type

    private static IntWritable ONE = new IntWritable(1);
    private static StringToIntMapWritable stripe = new StringToIntMapWritable();
    private int window = 10;

    @Override
    public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\s+"); //split string to tokens

        for(int i = 0; i < words.length; i++) {
            if (words.length == 0)
                continue;
            for (int j = i - window; j < i + window; j++) {
                if (i == j || j < 0)
                    continue;
                else if (j >= words.length)
                    break;
                else if (words[j].length() == 0) //skip empty tokens
                    break;
                else {
                    stripe.setStringToIntMapWritable(words[j], 1);
                }
            }
            context.write(new Text(words[i]), stripe);
            stripe.clean();
        }

    // TODO: implement map method
  }
}

class StripesReducer
extends Reducer<Text, StringToIntMapWritable, Text, StringToIntMapWritable> { // TODO: change Object to output value type

    @Override
    public void reduce(Text key, Iterable<StringToIntMapWritable> values, Context context) throws IOException, InterruptedException {

        StringToIntMapWritable sum_stripe = new StringToIntMapWritable();
        for(StringToIntMapWritable stripe: values) {
            sum_stripe.add(stripe);
        }
        context.write(key, sum_stripe);

    // TODO: implement the reduce method
  }
}