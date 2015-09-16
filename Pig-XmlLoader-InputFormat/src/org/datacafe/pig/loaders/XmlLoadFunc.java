package org.datacafe.pig.loaders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class XmlLoadFunc extends LoadFunc {

	private RecordReader<LongWritable, BookWritable> reader;
	private TupleFactory tupleFactory;
	
	public XmlLoadFunc() {
		tupleFactory = TupleFactory.getInstance();
	}
	
	@Override
	public InputFormat<LongWritable, BookWritable> getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new XMLInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		Tuple tuple = null;
		List<Object> values = new ArrayList<Object>();
		boolean notDone;
		try {
			notDone = reader.nextKeyValue();
			if (!notDone) {
				return null;
			}
			BookWritable value = (BookWritable) reader.getCurrentValue();
			if (value != null) {
				values.add(value.getBookAuthor().toString());
				values.add(value.getBookTitle().toString());
				values.add(value.getBookISBN().toString());
			}
			
			tuple = tupleFactory.newTuple(values);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tuple;
	}

	@Override
	public void prepareToRead(RecordReader arg0, PigSplit arg1)
			throws IOException {
		// TODO Auto-generated method stub
		this.reader = arg0;
	}

	@Override
	public void setLocation(String arg0, Job arg1) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.setInputPaths(arg1, arg0);
	}

}
