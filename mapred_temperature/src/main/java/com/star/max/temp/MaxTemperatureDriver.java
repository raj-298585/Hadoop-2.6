package com.star.max.temp;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.star.max.temp.mapper.MaxTempMapper;
import com.star.max.temp.reducer.MaxTempReducer;

/**
 * Hello world!
 * 
 */
public class MaxTemperatureDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf(
					"Usage: %s [generic options] <input>  <output>\n ",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// Instantiate and Name the Job
		Job job = new Job(getConf(), "Max Temperature");
		job.setJarByClass(getClass());

		// Set Input and Output Paths for the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Specify Mapper and Reducers for the job
		job.setMapperClass(MaxTempMapper.class);
		job.setReducerClass(MaxTempReducer.class);

		// Specify the Mapper
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Submit the job to hadoop and wait for it's completion
		return job.waitForCompletion(true) ? 0 : 1;
	}
}