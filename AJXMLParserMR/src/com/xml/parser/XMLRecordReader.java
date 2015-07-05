package com.xml.parser;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XMLRecordReader extends RecordReader<LongWritable, BookWritable> {

	public static final String START_TAG_KEY = "<book>";
	public static final String END_TAG_KEY = "</book>";
	private FSDataInputStream fsin;
	private byte[] startTag;
	private byte[] endTag;
	private long endPos = 0;
	private long startPos = 0;
	private long start;
	private long end;
	private LongWritable key;
	private BookWritable value = new BookWritable();
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private Map<Long, BookWritable> mapBooks = new HashMap<Long, BookWritable>();
	String xmlRecords = "<books><book><author>Anurag Jain</author>"
        + "<title>Hadoop Recipes</title><ISBN>04567GHFR</ISBN></book></books>";
	private String xmlStubData = "<book>" + "<author>Anurag Jain</author>"
			+ "<title>Hadoop Recipes</title>" + "<ISBN>04567GHFR</ISBN>"
			+ "<price>45</price>" + "<publisher>Edureka</publisher></book>"
			+ "<book>" + "<author>Jim Corbett</author>"
			+ "<title>Hadoop Guide</title>" + "<ISBN>04098IUFR</ISBN>"
			+ "<price>35</price>" + "<publisher>Test</publisher></book>";

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fsin.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public BookWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (start == end) {
			return 0.0f;
		} else {
			return Math
					.min(1.0f, (startPos - start) / (float) (endPos - start));
		}
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = arg1.getConfiguration();

		FileSplit split = (FileSplit) arg0;
		start = split.getStart();
		
		System.out.println(start);
		end = start + split.getLength();
		System.out.println(end);
		final Path file = split.getPath();
		System.out.println(file.toString());
		FileSystem fs = file.getFileSystem(conf);
		fsin = fs.open(file);
		fsin.seek(start);

		DocumentBuilder db = null;
		try {
			db = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//InputSource is = new InputSource();
		//is.setCharacterStream(new StringReader(xmlRecords));

		Document doc = null;
		try {
			doc = db.parse(fsin);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		NodeList nodes = doc.getElementsByTagName("book");

		//<books><book><author>Anurag Jain</author>"
        //"<title>Hadoop Recipes</title><ISBN>04567GHFR</ISBN></book></books>
		
		
		for (int i = 0; i < nodes.getLength(); i++) {
			Element element = (Element) nodes.item(i);
			BookWritable book = new BookWritable();
			NodeList author = element.getElementsByTagName("author");
			Element line = (Element) author.item(0);
			System.out.println("this" + getCharacterDataFromElement(line));
			book.setBookAuthor(new Text(getCharacterDataFromElement(line)));

			NodeList title = element.getElementsByTagName("title");
			line = (Element) title.item(0);
			System.out.println("is" + getCharacterDataFromElement(line));
			book.setBookTitle(new Text(getCharacterDataFromElement(line)));

			NodeList isbn = element.getElementsByTagName("ISBN");
			line = (Element) isbn.item(0);
			System.out.println("awesome" + getCharacterDataFromElement(line));
			book.setBookISBN(new Text(getCharacterDataFromElement(line)));

			mapBooks.put(Long.valueOf(i), book);
		}
		this.startPos = 0;
		endPos = mapBooks.size();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		if (key == null) {
			key = new LongWritable();
		}
		
		//key = 0
		key.set(startPos);
		if (value == null) {
			value = new BookWritable();
		}
		if (startPos >= endPos) {
			key = null;
			value = null;
			return false;
		} else {
			value = mapBooks.get(startPos);
			startPos = startPos + 1;
			return true;
		}
	}

	public static String getCharacterDataFromElement(Element e) {
		Node child = e.getFirstChild();
		if (child instanceof CharacterData) {
			CharacterData cd = (CharacterData) child;
			return cd.getData();
		}
		return "";
	}
}
