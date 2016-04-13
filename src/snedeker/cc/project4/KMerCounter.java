package snedeker.cc.project4;

import java.io.IOException;

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

public class KMerCounter {
	
	/**
	 * This is the Mapper component.  It will take the list of users and friends and use them
	 * to generate user,user friends pairs for every user a user is friends with.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    //hadoop supported global variables
	    private final static IntWritable one = new IntWritable(1);
	    private static String previousLine;
		
		/**
		 * This is the map function.  In this function the lines are read and tokenized.  The 
		 * user and friend information is converted to a series of pairs.  The keys for these 
		 * pairs consist of the user and each friend, and the value is the complete list of
		 * friends.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Read in the first line
			String line = value.toString();
			
			Configuration conf = context.getConfiguration();
			int kLength = Integer.parseInt(conf.get("k-mer.length"));
			
			if (!line.startsWith(">")) {
				if (previousLine != null && !previousLine.isEmpty()) {
					for (int i = kLength - 1; i > 0; i--) {
						context.write(new Text(previousLine.substring(previousLine.length() - i) + line.substring(0, kLength - i)), one);
					}
				}
				
				for (int i = 0; i <= (line.length() - 10); i++) {
					context.write(new Text(line.substring(i, i + kLength)), one);
				}
			}
			
			previousLine = line;
		}
	}
	
	/**
	 * This is the Reducer component.  It will take the Mapped, Shuffled, and Sorted data,
	 * and generate the lists of mutual friends for each user.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		/**
		 * This is the reduce function.  It takes all of the entries for this (i,k) 
		 * position of the result matrix, sorts them by j value, multiplies the 
		 * matching pairs, and sums the results together to find the value to place
		 * into the resultant matrix.
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			context.write(key, new IntWritable(sum));
		}
	}
	
	/**
	 * Configures the Hadoop job, and reads the user provided arguments
	 * 
	 * @param args The user provided arguments.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//Get configuration object and set a job name
		Configuration conf = new Configuration();
		conf.set("k-mer.length", args[0]);
		conf.set("mapred.textoutputformat.separator", "");
		Job job = new Job(conf, "kMerCounter");
		job.setJarByClass(snedeker.cc.project4.KMerCounter.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//Set key, output classes for the job (same as output classes for Reducer)
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//Set format of input files; "TextInputFormat" views files
		//as a sequence of lines
		job.setInputFormatClass(TextInputFormat.class);
		//Set format of output files: lines of text
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setNumReduceTasks(2); #set num of reducers
		//accept the hdfs input and output directory at run time
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		//Launch the job and wait for it to finish
//		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
