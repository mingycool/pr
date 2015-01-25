package pr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	private double dampFactor = 0.85;
	private Text outputValue = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		String outlinks = "";
		for (Text val : values) {
			String temp = val.toString();
			if (temp.indexOf(":") == 0)
				outlinks = temp.substring(temp.indexOf(":") + 1,
						temp.length());
			else
				sum += Double.valueOf(temp);
		}
		sum = dampFactor * sum + (1 - dampFactor);
		outputValue.set(String.valueOf(sum) + outlinks);
		context.write(key, outputValue);

	}
}
