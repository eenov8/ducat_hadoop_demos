package com.web.logs;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebAnalyticsDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		BufferedReader reader = new BufferedReader(
				//new FileReader("hdfs://localhost:54310/tmp/webdata/datasets/ip_data_sample.csv"));
				new FileReader("/home/edureka/Desktop/webLogs/ip_data.csv"));
		String ip_data = "";
		String line = "";
		while((line = reader.readLine()) != null){
			System.out.println(line.toString());
			ip_data = ip_data + "@@@" + line.toString();
		}
		System.out.println(ip_data);
		conf.set("ip_data", ip_data);
		
		Job job = Job.getInstance(conf, "WebAnalyticsMRJob");
		job.setJarByClass(com.web.logs.WebAnalyticsDriver.class);
		job.setMapperClass(com.web.logs.WebAnalyticsMapper.class);
		job.setReducerClass(com.web.logs.WebAnalyticsReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
///tmp/mockusWeblogsSample.txt /tmp/logs_out/
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:8020/tmp/mockusWeblogsSample.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:8020/tmp/logs_out"));

		if (!job.waitForCompletion(true))
			return;
	}
}