package com.test.msword;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.poi.POIOLE2TextExtractor;
import org.apache.poi.extractor.ExtractorFactory;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.xmlbeans.XmlException;

public class MsWordRecordReader extends RecordReader<LongWritable, Text> {

	private int maxLineLength;
	private Map<Long, String> mapParagraphs = new HashMap<Long, String>();
	private long start = 0;
	private long end = 0;
	private long pos = 0;
	private LongWritable key;
	private Text value = new Text();
	private LineReader in;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (in != null) {
			in.close();
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		boolean skipFirstLine = false;

		Configuration conf = arg1.getConfiguration();
		this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);

		FileSplit split = (FileSplit) arg0;
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream filein = fs.open(file);

		// below code is word doc specific
		POIFSFileSystem fileSystem = new POIFSFileSystem(filein);
		POIOLE2TextExtractor oleTextExtractor = null;
		WordExtractor wordExtractor = null;

		try {
			oleTextExtractor = ExtractorFactory.createExtractor(fileSystem);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (oleTextExtractor instanceof WordExtractor) {
			wordExtractor = (WordExtractor) oleTextExtractor;
		}

		// mapParagraphs
		String[] paragraphText = wordExtractor.getParagraphText();
		long count = 0;
		for (String paragraph : paragraphText) {
			mapParagraphs.put(count, paragraph);
			count++;
		}

		this.pos = 0;
		end = mapParagraphs.size();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		value.clear();
		if (pos >= end) {
			key = null;
			value = null;
			return false;
		} else {
			value = new Text(mapParagraphs.get(pos));
			pos = pos + 1;
			return true;
		}
	}

}
