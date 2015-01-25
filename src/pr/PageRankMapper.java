package pr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outKey = new Text(); // output key
	private Text outValue = new Text(); // output value
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); // convert Text to string
		int separateIndex = line.indexOf("\t"); //
		String nodeId = line.substring(0, separateIndex);
		String rest = line.substring(separateIndex, line.length()); // get the
																	// rest of
																	// the line
		String[] valuesList = rest.split("\t"); // the array contains the
												// PageRank and the outlinks
		String pagerank = valuesList[1]; // the first element is empty

		StringBuilder sb = new StringBuilder(); // save the outlinks

		int numOfNodes = valuesList.length - 2;
		Double outputPageRank = Double.parseDouble(pagerank) / numOfNodes;
		outValue.set(outputPageRank.toString());
		for (int i = 2; i < valuesList.length; i++) { // start from the third
														// element since the
														// second is PageRank
			outKey.set(valuesList[i]);
			context.write(outKey, outValue);
			sb.append(valuesList[i] + "\t");
		}
		context.write(new Text(nodeId),
				new Text(":" + sb.toString()));
	}
}
