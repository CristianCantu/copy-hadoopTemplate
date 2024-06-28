package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelayReducer extends  Reducer<Text, Text, Text, FloatWritable> {

   private TreeMap<Float, Text> topAirlines = new TreeMap<>();

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
            int totalDelay = 0;
            int totalCount = 0;
    
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalDelay += Integer.parseInt(parts[0]);
                totalCount += Integer.parseInt(parts[1]);
            }
    
            if (totalCount > 0) {
                float delayRatio = (float) totalDelay / totalCount;
                topAirlines.put(delayRatio, new Text(text));
    
                if (topAirlines.size() > 3) {
                    topAirlines.remove(topAirlines.firstKey());
                }
            }
   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
    for (Map.Entry<Float, Text> entry : topAirlines.descendingMap().entrySet()) {
        context.write(entry.getValue(), new FloatWritable(entry.getKey()));
    }
}
}