package com.star.max.temp;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.star.max.temp.reducer.MaxTempReducer;

public class MaxTempReducerTest {
	@Test
	public void returnsMaxIntegerInValues() throws IOException,
			InterruptedException {
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
				.withReducer(new MaxTempReducer())
				.withInput(new Text("1950"),
						Arrays.asList(new IntWritable(10), new IntWritable(5)))
				.withOutput(new Text("1950"), new IntWritable(10)).runTest();
	}
}
