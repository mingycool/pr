package pr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); // configurate a new conf to
													// generate graph
		conf.set("confname", "GraphChracter");
        Job job = new Job(conf);
        job.setJobName("GraphCharacter");
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);
        job.setJarByClass(PageRank.class);
        Path in = new Path(args[0] + "/Input" + "/"); // always work on the path
                                                        // of the previous
                                                        // iteration
        Path out = new Path(args[0] + "/Result/Graph");
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true); // wait for completion and update the
                                        // counter
        Configuration gconf = new Configuration(); 
		gconf.set("confname", "GenerateGraph");
		Job gjob = new Job(conf);
		gjob.setJobName("GenrateGraph");
		gjob.setMapperClass(FirstPRMapper.class);
		gjob.setReducerClass(FirstPRReducer.class);
		gjob.setJarByClass(PageRank.class);
		out = new Path(args[0] + "/Temp/Iteration_0");
		FileInputFormat.addInputPath(gjob, in);
		FileOutputFormat.setOutputPath(gjob, out);
		gjob.setInputFormatClass(TextInputFormat.class);
		gjob.setOutputFormatClass(TextOutputFormat.class);
		gjob.setOutputKeyClass(Text.class);
		gjob.setOutputValueClass(Text.class);
		gjob.waitForCompletion(true); // wait for completion and update the
										// counter
		String Result = "\nRESULT: \n\nITERATIONS   TIME(Millisecond)\n\n";
		int iteration = 1;
		int ITERATIONS = Integer.parseInt(args[1]);
		long totalTime=0;
		long tentime=0;
		while (iteration <= ITERATIONS) {

			long startTime = System.currentTimeMillis();
			// reuse the conf reference with a fresh object
			Configuration nconf = new Configuration();
			// set the depth into the configuration
			nconf.set("recursion.depth", iteration + "");
			Job njob = new Job(nconf);
			njob.setJobName("PageRank_" + iteration);
			njob.setMapperClass(PageRankMapper.class);
			njob.setReducerClass(PageRankReducer.class);
			njob.setJarByClass(PageRank.class); // always work on the path of the
												// previous depth
			in = new Path(args[0] + "/Temp/Iteration_" + (iteration - 1) + "/");
			out = new Path(args[0] + "/Temp/Iteration_" + iteration);
			FileInputFormat.addInputPath(njob, in);
			FileOutputFormat.setOutputPath(njob, out);
			njob.setInputFormatClass(TextInputFormat.class);
			njob.setOutputFormatClass(TextOutputFormat.class);
			njob.setOutputKeyClass(Text.class);
			njob.setOutputValueClass(Text.class);
			// wait for completion and update the counter
			njob.waitForCompletion(true);
			long finishTime = System.currentTimeMillis();
			totalTime+=finishTime - startTime;		
			if (iteration==10)
			    tentime=totalTime;
			iteration++;
		}
		Configuration sconf = new Configuration();
		Job sjob = new Job(sconf);
		sjob.setJobName("Sorting");
		sjob.setMapperClass(PageRankSortMapper.class);
		sjob.setReducerClass(PageRankSortReducer.class);
		sjob.setJarByClass(PageRank.class);
		sjob.setNumReduceTasks(1);
		in = new Path(args[0] + "/Temp/Iteration_" + (iteration - 1) + "/");
		out = new Path(args[0] + "/Result/Sorted");
		FileInputFormat.addInputPath(sjob, in);
		FileOutputFormat.setOutputPath(sjob, out);
		sjob.setInputFormatClass(TextInputFormat.class);
		sjob.setOutputFormatClass(TextOutputFormat.class);
		sjob.setOutputKeyClass(Text.class);
		sjob.setOutputValueClass(Text.class);
		sjob.waitForCompletion(true); // wait for completion and update the
										// counter

		
		out = new Path(args[0] + "/Result");
		try 
        {
            //FileSystem hdfs = FileSystem.get(uri, new Configuration());
            FileSystem hdfs = out.getFileSystem(new Configuration()); 
            Path inputresult=new Path(args[0] + "/Result/Sorted","part-r-00000");
            BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(inputresult)));
            Path pt = new Path(out+"/result", "result.txt");
            BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(hdfs.create(pt,true)));
            // write statistics   
            bw.write(String.format("First ten iterations execution time = %.3f seconds\n\n", (float)(tentime)/1000));
            bw.write(String.format("Total execution time = %.3f seconds\n\n", (float)(totalTime)/1000));
            bw.write("Top 10 nodes are as below: \n\n");
            String str = null;
            for (int i = 0; i < 10;i++)
            {   
                str=br.readLine();
                String[] s=str.split(" ");
                bw.write(s[0]+"\n");
            }
            bw.close();
            System.out.println("\n\nAll tasks have been finished!!\n\nThe result of top ten nodes and execution time is in /result/result.txt\n\nThe result of graph property is in /Graph\n\nThe result of the final sorted nodes list is in /Sorted");
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
        }	
		
	}

}
