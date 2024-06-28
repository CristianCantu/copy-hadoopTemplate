package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text flight = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
        String[] columns = value.toString().split(",");
		if(!columns[0].equals("YEAR")){
			flight.set(columns[7]);
			context.write(flight, counter);
		}
		
	}
}