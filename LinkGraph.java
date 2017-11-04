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


public class LinkGraph extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(LinkGraph.class);
	
	public static void main(String[] args) throws Exception {
	
     int res = ToolRunner.run(new LinkGraph(), args);
     System.exit(res);
  }
  
  public int run( String[] args) throws  Exception {
	  
	  
	  Job countJob = Job.getInstance(getConf(), " pageCountJob ");  // instantiate job
	  Path link_graph =new Path(args[2],"part-r-00000");
	  
	  countJob.setJarByClass( this.getClass());
	  countJob.setMapperClass(MapCount.class);
	  countJob.setReducerClass(ReduceCount.class);
	  
      

      FileInputFormat.addInputPaths(countJob,  args[0]); // input directory  (job, Inputpath)
      FileOutputFormat.setOutputPath(countJob,  new Path(args[1])); // output directory (job, outputPath)
	  
	  
      
	  // configure the output format
      countJob.setOutputKeyClass(Text .class);
      countJob.setOutputValueClass(IntWritable.class);

       int s = countJob.waitForCompletion(true) ? 0 : 1;   // on completion of the count job
	   
	   
	   if(s==0){ // if success
		   // start the LinkGraph job
		   
		   Job graphJob = Job.getInstance(getConf(), "linkGraphJob");
	  
		 Configuration config = graphJob.getConfiguration();
	  String line;
	  int numLines = 1;
	  
	  
	  
	  try {
				FileSystem fs = FileSystem.get(config);
				Path p = new Path(args[1], "part-r-00000");
				

				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
				while ((line = br.readLine()) != null) {
					if (line.trim().length() > 0) {
						
						String[] parts = line.split("\\s+");
						numLines = Integer.parseInt(parts[1]);
					}
				}
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
	  
	  
	  config.set("numOfLines", String.valueOf(numLines));
	  
	  graphJob.setJarByClass( this.getClass());
	  graphJob.setMapperClass(MapGraph.class);
	  graphJob.setReducerClass(ReduceGraph.class);
	  
      

      FileInputFormat.addInputPaths(graphJob,  args[0]); // input directory  (job, Inputpath)
      FileOutputFormat.setOutputPath(graphJob, link_graph ); // output directory (job, outputPath)
	  
	  
      
	  // configure the output format
      graphJob.setOutputKeyClass(Text.class);
      graphJob.setOutputValueClass(Text.class);
		   
		   s = graphJob.waitForCompletion(true) ? 0 : 1; 
		  
	   }
	   return  s; // on completion of the count job
   }
   // map reduce to compute the Number of the lines in the input file
   
   
   public static class MapCount extends Mapper<LongWritable ,Text,Text , IntWritable > {
      private final static IntWritable one  = new IntWritable(1);
      private Text word  = new Text();

		

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString().trim();
		 
		 if(!line.isEmpty()){
			context.write(word,one);
		 }
         
      }
   }
   
   
   public static class ReduceCount extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
			 
			 
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get(); // counting the lines in the document
         }
         context.write(new Text("numLines"),  new IntWritable(sum));
      }
   }
   
   
   public static class MapGraph extends Mapper<LongWritable ,Text,Text , Text > {
      
		private static final String SEP="####";

		private static final Pattern patternTitle = Pattern.compile("<title>(.*?)</title>"); // pattern to extract the title from the line
		private static final Pattern patternLink = Pattern.compile("<text(.*?)</text>"); // pattern to extract links corresponding to the title
		private static final Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
		 String linCounts = context.getConfiguration().get("numOfLines");
		 Matcher mTitle = patternTitle.matcher(line);
		 String title="";
		 Double INIT = 1/Double.parseDouble(linCounts);
		 while(mTitle.find()){
			 title=mTitle.group(1); // grab the title 
		 }
		 
		 
		 Matcher mLink = patternLink.matcher(line);
		 String links;
		 String val =Double.toString(INIT)+"----";
		 
		 while(mLink.find()){
			 links=mLink.group(1); // grab the links list
			 Matcher m = pattern.matcher(links);
			 while(m.find()){
				 String url = m.group(1).replace("[[","").replace("]]","");
				 val+=url+SEP;     // 3####5####7
			 }
			 
		 }
		 if(!title.isEmpty() && !val.isEmpty()){
			 context.write(new Text(title),new Text(val));  // title, links -> 1, 3####5
		 }
		 
         
      }
   }
   public static class ReduceGraph extends Reducer<Text ,  Text ,  Text ,  Text > {
      
	  @Override 
      public void reduce( Text word,  Iterable<Text> counts,  Context context)
         throws IOException,  InterruptedException {
			 
										// word -> title
        for(Text c : counts){     //c -> 3####5
			
			
			
			
			
         context.write(word, c);  //-->  1, 3####5
		}
		 }
   
   
   
   
   
   
   
   
   
   
   
}
}
