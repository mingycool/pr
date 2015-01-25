package pr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PageRankSortReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            Double result = Double.parseDouble(key.toString());
            result = 1 / result;
            context.write(value,new Text(String.valueOf(result))); // to emit
                                                                    // actual
                                                                    // rank and
                                                                    // node
        }
    }
}
