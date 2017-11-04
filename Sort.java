/* Sivalingam Subbiah 
ssubbiah@uncc.edu */
package org.myorg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



public class Sort extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(Sort.class);
	
	public static void main(String[] args) throws Exception {
	
     int res = ToolRunner.run(new Sort(), args);
     System.exit(res);
  }
  
  public int run( String[] args) throws  Exception {
	  
	  
	  Job sortJob = Job.getInstance(getConf(), " pagesortJob ");  // instantiate job
	  
	  
	  sortJob.setJarByClass( this.getClass());
	  sortJob.setMapperClass(MapPageRankSort.class);
	  sortJob.setReducerClass(ReducePageRankSort.class);
	  
	  
	  sortJob.setNumReduceTasks(1);  // number of possible reducers
      

      FileInputFormat.addInputPaths(sortJob,  args[0]); // input directory  (job, Inputpath)
      FileOutputFormat.setOutputPath(sortJob,  new Path(args[1])); // output directory (job, outputPath)
	  
	  
      
	  // configure the output format
				sortJob.setInputFormatClass(KeyValueTextInputFormat.class);
				sortJob.setOutputFormatClass(TextOutputFormat.class);
				
				sortJob.setSortComparatorClass(PageRankComparator.class);
				
				sortJob.setMapOutputKeyClass(DoubleWritable.class);
				sortJob.setMapOutputValueClass(Text.class);
				sortJob.setOutputKeyClass(Text.class);
				sortJob.setOutputValueClass(DoubleWritable.class);

       return sortJob.waitForCompletion(true) ? 0 : 1;   // on completion of the count job
	   
	   
	   
  }

public static class MapPageRankSort extends Mapper<Text, Text, DoubleWritable, Text> {

		public void map(Text url, Text lineText, Context context) throws IOException, InterruptedException {
			/*
			split at the delimiter, discard outlinks, get the pagerank and write to output
			*/
			String[] temp = lineText.toString().split("----"); 

			// use rank as key and url as value to help sort
			if (temp.length > 0)
				context.write(new DoubleWritable(Double.valueOf(temp[0])), url);

		}
	}
	
	
	
	public static class ReducePageRankSort extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable rank, Iterable<Text> urls, Context context)
				throws IOException, InterruptedException {

			// loop through the list of urls for each value
			// in case multiple urls have same rank, sort and print
			for (Text url : urls) {
				context.write(url, rank);
			}
		}
	}
	
	// custom comparator for sorting rank values in descending order
	public static class PageRankComparator extends WritableComparator {

		public PageRankComparator() {
			super();
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
			// TODO Auto-generated method stub

			double rank1 = WritableComparator.readDouble(arg0, arg1);
			double rank2 = WritableComparator.readDouble(arg3, arg4);
			if (rank1 > rank2) {
				return -1;
			} else if (rank1 < rank2) {
				return 1;
			}
			return 0;
		}
	}
	
  }
