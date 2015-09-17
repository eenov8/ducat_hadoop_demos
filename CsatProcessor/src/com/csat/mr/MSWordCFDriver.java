package com.csat.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MSWordCFDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
	    //estimate reducers
	    Job job = Job.getInstance(conf, "MS CF");
	    job.setJarByClass(com.csat.mr.MSWordCFDriver.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setInputFormatClass(com.csat.mr.CFInputFormat.class);
	    job.setMapperClass(com.csat.mr.MSWordCFMapper.class);
	    job.setReducerClass(com.csat.mr.MSWordCFReducer.class);
	    //job.setNumReduceTasks(0);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    Path output=new Path(args[1]);
	    try {
	        fs.delete(output, true);
	    } catch (IOException e) {
	        //LOG.warn("Failed to delete temporary path", e);
	    }
	    FileOutputFormat.setOutputPath(job, output);

	    boolean ret=job.waitForCompletion(true);
	    if(!ret){
	        throw new Exception("Job Failed");
	    }
	}
}
