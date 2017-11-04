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


public class PageRank extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(PageRank.class);
	
	public static void main(String[] args) throws Exception {
	
     int res = ToolRunner.run(new PageRank(), args);
     System.exit(res);
  }
  
  public int run( String[] args) throws  Exception {
	  int s=0;
	  
	  Path link_graph =new Path(args[0],"part-r-00000");  // provide the path where linkgraph output is present
	  Path inter_path = new Path(args[1],"part-r-00000");
	  Path newInter_path=link_graph;
	  //Path newInter_path = new Path(args[2], "iter"+2);
	  
	  
	  
	  for(int i=1; i<11; i++ ){
		  if(s==0){
		  Job jobPageRank = Job.getInstance(getConf(), " jobPageRank ");  // instantiate job
		  Configuration confPR = jobPageRank.getConfiguration();
		  
		  jobPageRank.setJarByClass( this.getClass());
	  jobPageRank.setMapperClass(MapRank.class);
	  jobPageRank.setReducerClass(ReduceRank.class);
		  
		  
		  
		  
		  FileInputFormat.addInputPaths(jobPageRank, link_graph.toString());
		  FileOutputFormat.setOutputPath(jobPageRank, inter_path);
		  
		  
		  
		  FileSystem fsPR = FileSystem.get(confPR);
		  
		  try {
						if (fsPR.exists(inter_path)) {
							fsPR.delete(inter_path, true);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
		  
		  
		  jobPageRank.setInputFormatClass(KeyValueTextInputFormat.class);
                  jobPageRank.setOutputFormatClass(TextOutputFormat.class);

		  
		  // configure the output format
      jobPageRank.setOutputKeyClass(Text.class);
      jobPageRank.setOutputValueClass(Text.class);
	  
	  
	  
	  
	  s = jobPageRank.waitForCompletion(true) ? 0 : 1;   // on completion of the count job
	  link_graph=inter_path;
	  inter_path=newInter_path;
	  newInter_path = link_graph;
		  }
	  }
	  
	
	   
	   
	   return  s; // on completion of the count job
   }
   // map reduce to compute the Number of the lines in the input file
   
   
   public static class MapRank extends Mapper<Text ,Text,Text , Text > {
     
      

		

      public void map( Text offset,  Text lineText,  Context context)   // offset -> url
        throws  IOException,  InterruptedException {

         String[] lineContent = lineText.toString().split("----");  //  1/N , urlOutlinks
		 String pr =lineContent[0];
		 if(lineContent.length==2){
		 String[] outLinksList = lineContent[1].split("####");
		 
		 for(String outLink : outLinksList){
			if(!outLink.isEmpty() && !pr.isEmpty()){
				context.write(new Text(outLink), new Text(String.valueOf(Double.parseDouble(pr)/(double)outLinksList.length)));
			} 
		 }
		 
		 if(!offset.toString().isEmpty() && !lineText.toString().isEmpty()){
			 context.write(offset,lineText);
		 }
		 }
         
      }
   }
   
   
   public static class ReduceRank extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text url,  Iterable<Text> counts,  Context context)   // url -> a page and it's collection of related values are sent
         throws IOException,  InterruptedException {
			
			boolean hasLinkedList = false;
			String outLink="";
			double pgNew =0.0;
			for(Text cou : counts){
				// collection of values which contains the oldRank with hyperList and also calcualted new PageRank
				String c = cou.toString();
				if(c.contains("----")){
					String[] outLinkList =  c.split("----");
					if(outLinkList.length ==2){
						outLink=outLinkList[1];
						hasLinkedList=true;
					}
				}
				else{
					pgNew+=Double.parseDouble(c); // compute the pr using all the extracted and computed values for the key.
					
				}	
			}
			
			double pgRank = (1-0.85) + 0.85*pgNew;  // compute the PageRank for the Page. 0.85 --> damping factor given
			
			if(!url.toString().isEmpty() && hasLinkedList){
				String str = pgRank+"----"+outLink; // preparing the value so that the mapper can again consume it to compute
				context.write(url,new Text(str));
			}
      }
   }
} 
   
