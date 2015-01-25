package pr;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphReducer
extends Reducer<Text,Text, Text, Text> 
{
    public void reduce(Text key, Iterable<Text> values,
            Context context
            ) throws IOException, InterruptedException 
    {
        int sumEdges = 0, num = 0;
        int maxEdge = 0;  
        int minEdge = Integer.MAX_VALUE;
        String outputEdge = new String();
        for (Text val : values)
        {
            String valStr = val.toString();
            String nodeId = valStr.split(":")[0];
            String edge = valStr.split(":")[1];
            int nedge = Integer.parseInt(edge);
            outputEdge += String.format("node [%s] has out-degree = %d\n", nodeId, nedge);
            sumEdges += nedge;
            if (nedge > maxEdge)
                maxEdge = nedge;
            if (nedge < minEdge)
                minEdge = nedge;
            num++;
        }        
        float avgEdges = 0;
        if (num != 0) 
            avgEdges = (float) sumEdges / num;  
        String Outvalue = String.format("\n\nNumber of total nodes = %d\n", num);
        Outvalue += String.format("Number of total edges  = %d\n", sumEdges);
        Outvalue += String.format("Min out-degree = %d\n",  minEdge);
        Outvalue += String.format("Max out-degree = %d\n", maxEdge);
        Outvalue += String.format("Average of out-degree = %f\n\n", avgEdges);
        Outvalue += "Details of each node's out-degrees:\n\n";
        Outvalue += outputEdge;        
        context.write(new Text("Graph Information:"), new Text(Outvalue));
    }
}