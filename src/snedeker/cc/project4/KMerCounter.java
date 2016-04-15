package snedeker.cc.project4;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

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
	 * This is the Mapper component.  It reads the lines, composes the kmers, and outputs them to the context.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    //hadoop supported global variables
	    private final static IntWritable one = new IntWritable(1);
	    private static String previousLine;
		
		/**
		 * This is the map function.  In this function the lines are read.  Substrings are
		 * taken from each line for the length of the kmer specified in the configuration.
		 * These are then output to the context as a key, value pair consisting of the kmer
		 * as the key, and a hard coded one value as the value.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Read in the first line
			String line = value.toString();
			
			Configuration conf = context.getConfiguration();
			int kLength = Integer.parseInt(conf.get("k-mer.length"));
			
			if (!line.startsWith(">")) {
				if (previousLine != null && !previousLine.isEmpty()) {
					int j = 0;
					for (int i = kLength - 1; i > 0 && j < line.length(); i--) {
						String previousPart = previousLine.substring(previousLine.length() - i);
						String currentPart = line.substring(0, j + 1);
						
						context.write(new Text( previousPart + currentPart), one);
						j++;
					}
				}
				
				for (int i = 0; i <= (line.length() - kLength); i++) {
					context.write(new Text(line.substring(i, i + kLength)), one);
				}
			}
			
			previousLine = line;
		}
	}
	
	/**
	 * This is the Reducer component.  It will receive the Mapped, Shuffled, and Sorted kmers.
	 * The kmers are then summed to find the total count, and ordered to find the top 10.
	 * 
	 * @author Colby Snedeker
	 *
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private TreeMap<Integer, PriorityQueue<String>> orderer = new TreeMap<>();

		/**
		 * This is the reduce function.  It takes all of the entries containing a kmer
		 * and it's hard coded one value then sums them to find the total count for
		 * each kmer.  Once this sum has been found the kmer is inserted into a priority
		 * queue which is the value of a TreeMap entry where the sum is the key.  This
		 * provides automatic sorting first on the count, and then alphabetically.  It
		 * then reads through the top 10 entries, and outputs them to the context.
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			PriorityQueue<String> kmers = orderer.get(Integer.valueOf(sum));
			
			if (kmers != null) {
				kmers.add(key.toString());
			}
			else {
				PriorityQueue<String> newKmer = new PriorityQueue<>();
				newKmer.add(key.toString());
				orderer.put(Integer.valueOf(sum), newKmer);
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int i = 0;
			
			for (Entry<Integer, PriorityQueue<String>> entry : orderer.descendingMap().entrySet()) {
				for (String kmer : entry.getValue()) {
					if (i < 10) {
						i++;
						
						context.write(new Text(kmer), new IntWritable(entry.getKey()));
					}
				}
			}
			
//			for (Entry<Text, Integer> entry : kMerTreeMap.entrySet()) {
//				context.write(entry.getKey(), new IntWritable(entry.getValue()));
//			}
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
