package com.star.max.temp;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class MaxTempDriverTest {

	@Test
	public void testMaxTempDriver() throws Exception {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "file:///");
		config.set("mapreduce.framework.name", "local");
		config.setInt("mapreduce.task.io.sort.mb", 1);

		Path input = new Path("src/main/resources/input/ncdc");
		Path output = new Path("output");

		FileSystem fs = FileSystem.getLocal(config);
		fs.delete(output, true);

		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(config);

		int exitCode = driver.run(new String[] { input.toString(),
				output.toString() });
		assertThat(exitCode, is(0));
	}
}
