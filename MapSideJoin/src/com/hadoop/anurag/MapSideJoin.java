package com.hadoop.anurag;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class MapSideJoin extends Configured implements Tool {

	Path inputPath = null;
	Path outputPath = null;
	public static final String COL_POS = "col_pos";

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			inputPath = new Path("hdfs://localhost:54310/tmp/mockus.logs");
			outputPath = new Path(
					"hdfs://localhost:54310/tmp/ip_city_out_class");
		} else {
			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);
		}
		Configuration conf = getConf();

		String India_ip_data = "";
		BufferedReader reader = new BufferedReader(new FileReader(
				"/home/hduser/ip_city.csv"));
		String line = reader.readLine();
		India_ip_data = India_ip_data + line.toString();
		while (line != null) {
			String[] tokens = line.split(",");
			String country_name = tokens[4];
			if (country_name.trim().equalsIgnoreCase("India")) {
				India_ip_data = India_ip_data + "@@@" + line.toString();
			}
			line = reader.readLine();
		}
		System.out.println(India_ip_data);
		reader.close();
		conf.set(COL_POS, India_ip_data);

		Job weblogJob = new Job(conf);
		weblogJob.setJobName("MapSideJoin");
		weblogJob.setNumReduceTasks(1);
		weblogJob.setJarByClass(getClass());
		weblogJob.setMapperClass(WeblogMapper.class);
		weblogJob.setOutputKeyClass(Text.class);
		weblogJob.setOutputValueClass(IntWritable.class);
		weblogJob.setInputFormatClass(TextInputFormat.class);
		weblogJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(weblogJob, inputPath);
		FileOutputFormat.setOutputPath(weblogJob, outputPath);
		if (weblogJob.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int returnCode = ToolRunner.run(new MapSideJoin(), args);
		System.exit(returnCode);
	}

}