package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
      job.setMapOutputKeyClass(StringToIntMapWritable.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setReducerClass(StripesReducer.class);
      job.setOutputKeyClass(StringToIntMapWritable.class);
      job.setOutputValueClass(IntWritable.class);

      job.setOutputFormatClass(TextOutputFormat.class);

      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[1])); //Why?? from different lib, what's the difference
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      job.setNumReduceTasks(Integer.parseInt(args[0]));

      job.setJarByClass(Stripes.class);
      return job.waitForCompletion(true) ? 0 : 1;
    // TODO: set job input format
    // TODO: set map class and the map output key and value classes
    // TODO: set reduce class and the reduce output key and value classes
    // TODO: set job output format
    // TODO: add the input file as job input (from HDFS) to the variable inputFile
    // TODO: set the output path for the job results (to HDFS) to the variable outputPath
    // TODO: set the number of reducers using variable numberReducers
    // TODO: set the jar class

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
extends Mapper<IntWritable, Text, Text, StringToIntMapWritable> { // TODO: change Object to output value type

    private static IntWritable ONE = new IntWritable(1);
    private static StringToIntMapWritable stripe = new StringToIntMapWritable();

    @Override
    public void map(IntWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\s+"); //split string to tokens

        for(int i = 0; i < words.length; i++) {
            for(int j = 0; j < words.length; j++) {
                if(!(words[i].equals(words[j]))) {
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