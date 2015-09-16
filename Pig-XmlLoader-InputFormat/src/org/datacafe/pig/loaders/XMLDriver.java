package org.datacafe.pig.loaders;

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
		conf.set("START_TAG_KEY", "<book>");
		conf.set("END_TAG_KEY", "</book>");
		Job job = Job.getInstance(conf, "XML");
		job.setJarByClass(org.datacafe.pig.loaders.XMLDriver.class);
		job.setMapperClass(org.datacafe.pig.loaders.XMLMapper.class);

		job.setReducerClass(org.datacafe.pig.loaders.XMLReducer.class);
		//job.setCombinerClass(org.datacafe.pig.loaders.XMLReducer.class);
		
		job.setInputFormatClass(XMLInputFormatPattern.class);
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		//  /tmp/sampleBookXml /tmp/xml-out/
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:8020/ericsson/books_large.xml"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/ericsson/xml-out-1"));

		if (!job.waitForCompletion(true))
			return;
	}

}
