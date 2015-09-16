package org.datacafe.pig.loaders;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XMLMapper extends Mapper<LongWritable, BookWritable, Text, IntWritable> {

	public void map(LongWritable ikey, BookWritable ivalue, Context context)
			throws IOException, InterruptedException {
		System.out.println(ivalue.getBookAuthor().toString() + "***************");
		context.write(ivalue.getBookAuthor(), new IntWritable(1));
	}

}
