-- SUMMARY --

This is a README file for PageRank program. program includes main and run methods. It includes 3 jar files with multiple jobs run sequentially in order to compute the page rank. First job is used to count the total number of pages in the document and the Second job is used to parse links and create a link graph from the Wikipedia pages, the next job calculates the page ranks iteratively from the existing page rank values, and the final job sanitizes, sorts according to the page rank values and outputs the Page rank values of the top 100 pages in the wiki document and saves the results to the output location in HDFS.

-- REQUIREMENTS --

HADOOP Environment or Cloudera VM.


-- Running the program --

* Before you run the sample, you must create input and output locations in HDFS. Use the following commands to create the input directory /user/cloudera/pagerank/input in HDFS:
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/pagerank /user/cloudera/pagerank/input 

* Move the text files of the Wikipedia corpus provided to use as input, and move them to the/user/cloudera/pagerank/input directory in HDFS. 

$ hadoop fs -put wiki* /user/cloudera/pagerank/input 

* Compile the Java files and Create a JAR file for the Compiled java files.
To compile in a package installation of CDH:

$ mkdir -p build

$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:. LinkGraph.java -d build -Xlint 

$ jar -cvf linkgraph.jar -C build/ . 

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:. PageRank.java -d build -Xlint 

$ jar -cvf pagerank.jar -C build/ . 

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/*:. Sort.java -d build -Xlint 
$ jar -cvf sort.jar -C build/ . 


* Run the PageRank application from the JAR file, by passing the paths to the input for Creating the link graph, the output path of documents count, the output path of Link Graph and the output path of page rank computation.

// To compute the count and link graph.
$ hadoop jar linkgraph.jar org.myorg.LinkGraph /user/cloudera/pagerank/input /user/cloudera/pagerank/outputCount /user/cloudera/pagerank/outputLink 

// To compute page rank using the link graph output. Set to run for 10 iterations
$ hadoop jar pagerank.jar org.myorg.PageRank /user/cloudera/pagerank/outputLink /user/cloudera/pagerank/outputRank

// Sort and organize the output generated from the pagerank  jar 
$ hadoop jar sort.jar org.myorg.Sort /user/cloudera/pagerank/outputRank/part-r-00000 /user/cloudera/pagerank/outputSort


* Output can be seen using the below command:
$ hadoop fs -cat /user/cloudera/pagerank/outputSort/*


* If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/pagerank/outputCount
$ hadoop fs -rm -r /user/cloudera/pagerank/outputLink
$ hadoop fs -rm -r /user/cloudera/pagerank/outputRank
$ hadoop fs -rm -r /user/cloudera/pagerank/outputSort


-- CONTACT --

* Sivalingam Subbiah (ssubbiah@uncc.edu)





 
