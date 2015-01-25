package pr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class FirstPRReducer extends Reducer<Text, Text, Text, Text> {
	private Text outputValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			outputValue = val;
		}
		context.write(key, outputValue);

	}
}