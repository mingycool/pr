package pr;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class FirstPRMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text outValue = new Text(); // output value

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();// convert Text to string
		StringTokenizer tokenizer = new StringTokenizer(line);
		if (tokenizer.hasMoreTokens()) {
			String nodeId = tokenizer.nextToken();
			StringBuilder output = new StringBuilder();
			output.append("0.2\t");
			while (tokenizer.hasMoreTokens()) {
				output.append(tokenizer.nextToken() + "\t");
			}
			outValue.set(output.toString());
			context.write(new Text(nodeId), outValue);
		}
	}
}
