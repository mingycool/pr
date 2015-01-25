package pr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PageRankSortMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// convert to string
		String line = value.toString();
		String[] valuesList = line.split("\t"); // split string into substrings												// based on deliminator
		String nodeId = valuesList[0]; // get current link
		String pagerank = valuesList[1]; // get the pagerank	
		Double pageRank = Double.parseDouble(pagerank); // converting string to													// double
		pageRank = 1 / pageRank;
		context.write(new Text(String.valueOf(pageRank)), new Text(
				nodeId)); 
	}
}
