package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {

        boolean success = runFlightJob(args[0], args[1]);
        if (!success) return 1;

        success = runDelayJob(args[0], args[2]);
        return success ? 0 : 1;
    }

    private boolean runFlightJob(String inputPath, String outputPath) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Flight Count");
            job.setJarByClass(WordCount.class);

            job.setMapperClass(FlightMapper.class);
            job.setReducerClass(FlightReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            return job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            System.err.println("Error during flight count job.");
            e.printStackTrace();
            return false;
        }
    }

    private boolean runDelayJob(String inputPath, String outputPath) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Delay Ratio");
            job.setJarByClass(WordCount.class);

            job.setMapperClass(DelayMapper.class);
            job.setReducerClass(DelayReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class); 

            FileInputFormat.addInputPath(job, new Path(inputPath));
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);

            return job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            System.err.println("Error during delay ratio job.");
            e.printStackTrace();
            return false;
        }
    }
}
