package Recommendation;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class Recommend extends Configured implements Tool {
	   public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new Recommend(), args);
	      
	      System.exit(res);
	   }

	   
	   
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "Recommend");
	      job.setJarByClass(Recommend.class);
	      job.setOutputKeyClass(LongWritable.class);
	      job.setOutputValueClass(FriendCountWritable.class);
	      
	   

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      //String a1="Relationship.txt";
	      //String a2="result";
	      
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	   }
}
	   

