package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightReducer extends  Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, Text> topAirports = new TreeMap<>();

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int sum = 0;
       
       for (IntWritable value : values) {
           sum += value.get();
       }
       topAirports.put(sum, new Text(text));
       if (topAirports.size() > 3) {
        topAirports.remove(topAirports.firstKey());
       }

   }

   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
       for (Map.Entry<Integer, Text> entry : topAirports.descendingMap().entrySet()) {
           context.write(entry.getValue(), new IntWritable(entry.getKey()));
       }
   }
}