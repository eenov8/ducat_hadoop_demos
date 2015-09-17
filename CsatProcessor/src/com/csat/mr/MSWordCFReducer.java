package com.csat.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MSWordCFReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text _key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		long sum = 0;
		for (LongWritable val : values) {
			sum = sum + val.get();
		}
		context.write(_key, new LongWritable(sum));
	}

}