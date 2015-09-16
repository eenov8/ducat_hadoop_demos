package org.datacafe.pig.loaders;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XMLInputFormatPattern extends FileInputFormat<LongWritable, BookWritable> {

	@Override
	public RecordReader<LongWritable, BookWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new XMLRecordReaderPattern((FileSplit) arg0,
		          arg1.getConfiguration());
	}

}
