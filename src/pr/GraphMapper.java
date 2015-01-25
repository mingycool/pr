package pr;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraphMapper
extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException 
    {
        String[] nodeIds = value.toString().split(" ");
        if (nodeIds[0].equals("") && nodeIds.length == 1)
            return; 
        int numOfEdges=nodeIds.length-1;
        if (nodeIds.length - 1 > 0 && nodeIds[1].equals(""))
            context.write(new Text("graph"), new Text(nodeIds[0]+":0"));
        else
            context.write(new Text("graph"), new Text(nodeIds[0]+":"+ String.valueOf(numOfEdges)));
    }
}