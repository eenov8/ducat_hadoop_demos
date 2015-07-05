package com.xml.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XMLDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("start_tag", "<book>");
		//conf.set("end_tag", "</book>");
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(com.xml.parser.XMLDriver.class);
		job.setMapperClass(com.xml.parser.XMLMapper.class);

		job.setReducerClass(com.xml.parser.XMLReducer.class);
		job.setCombinerClass(com.xml.parser.XMLReducer.class);
		
		job.setInputFormatClass(XMLInputFormat.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		//  /tmp/sampleBookXml /tmp/xml-out/
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:8020/tmp/loadMongo"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/tmp/xml-out-1"));

		if (!job.waitForCompletion(true))
			return;
	}

}
