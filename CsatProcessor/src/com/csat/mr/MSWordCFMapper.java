package com.csat.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MSWordCFMapper extends Mapper<Text, Text, Text, LongWritable> {

	public void map(Text ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		//System.out.println(ivalue.getBookAuthor().toString() + "***************");
		String type = "";
		if(ivalue.toString().contains("Poor")) {
			type = "poor";
		} else {
			type = "ok";
		}
		context.write(new Text(type), new LongWritable(1));
	}
}
