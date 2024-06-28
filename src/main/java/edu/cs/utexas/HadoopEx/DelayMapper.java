package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayMapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text airline = new Text();
    private Text delayAndCount = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
        String[] columns = value.toString().split(",");
        if (!columns[0].equals("YEAR")) {
            try {
                int delay = Integer.parseInt(columns[11]);
                airline.set(columns[4]);
                delayAndCount.set(delay + ", 1");
			    context.write(airline, delayAndCount);
            } catch (NumberFormatException e) {
                System.out.println("Not an integer");
            }
            
        }
			
		
	}
}