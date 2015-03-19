package com.test.msword;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MsWordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JobName");
        job.setJarByClass(com.test.msword.MsWordCount.class);
        job.setMapperClass(com.test.msword.MsWordMapper.class);
        job.setReducerClass(com.test.msword.MsWordReducer.class);

        // job.setInputFormatClass(WordInputFormat.class);
        job.setInputFormatClass(MsWordInputFormat.class);

        // TODO: specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
       
        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path(
                "hdfs://localhost:54310/tmp/drdo.doc"));
        FileOutputFormat.setOutputPath(job, new Path(
                "hdfs://localhost:54310/tmp/" + "msCSVOut"));

        System.out.println("test3");
        try {
            if (!job.waitForCompletion(true))
                return;
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

}