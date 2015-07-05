package com.web.logs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebAnalyticsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	int visitCount = 0;
	IntWritable output = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		for (IntWritable val : values) {
			visitCount += val.get();
		}
		output.set(visitCount);
		context.write(key, output);
	}
}
