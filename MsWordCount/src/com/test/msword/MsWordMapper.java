package com.test.msword;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MsWordMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable ikey, Text ivalue, Context context)
            throws IOException, InterruptedException {
        // taking one line at a time from input file and tokenizing the same
        String line = ivalue.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        // iterating through all the words available in that line and forming
        // the key value pair
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }

    }

}