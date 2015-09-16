package org.datacafe.pig.loaders;

import java.io.IOException;
import java.io.StringReader;

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

public class XMLRecordReaderPattern extends
		RecordReader<LongWritable, BookWritable> {

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

	// private Map<Long, BookWritable> mapBooks = new HashMap<Long,
	// BookWritable>();

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
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
	}

	public XMLRecordReaderPattern(FileSplit split, Configuration conf)
			throws IOException {
		startTag = START_TAG_KEY.getBytes("UTF-8");
		endTag = END_TAG_KEY.getBytes("UTF-8");

		// open the file and seek to the start of the split
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		fsin = fs.open(split.getPath());
		fsin.seek(start);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new LongWritable();
		value = new BookWritable();
		
		
		if (fsin.getPos() < end && readUntilMatch(startTag, false)) {
			try {
				buffer.write(startTag);
				if (readUntilMatch(endTag, true)) {
					key.set(fsin.getPos());
					
					Text test = new Text();
					test.set(buffer.getData(), 0, buffer.getLength());
					
					BookWritable book = new BookWritable();
					DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
				    InputSource is = new InputSource();
				    is.setCharacterStream(new StringReader(test.toString()));
				    
				    Document doc = db.parse(is);
				    NodeList nodes = doc.getElementsByTagName("book");

				    for (int i = 0; i < nodes.getLength(); i++) {
				      Element element = (Element) nodes.item(i);

				      NodeList name = element.getElementsByTagName("author");
				      Element line = (Element) name.item(0);
				      book.setBookAuthor(new Text(getCharacterDataFromElement(line)));

				      NodeList title = element.getElementsByTagName("title");
				      line = (Element) title.item(0);
				      book.setBookTitle(new Text(getCharacterDataFromElement(line)));
				      
				      NodeList isbn = element.getElementsByTagName("isbn");
				      line = (Element) isbn.item(0);
				      book.setBookISBN(new Text(getCharacterDataFromElement(line)));
				    }
					
				    //System.out.println("*******************" + book);
					value = book;
					return true;
				}
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				buffer.reset();
			}
		}
		return false;
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock)
			throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();
			// end of file:
			if (b == -1) {
				return false;
			}
			// save to buffer:
			if (withinBlock) {
				buffer.write(b);
			}

			// check if we're matching:
			if (b == match[i]) {
				i++;
				if (i >= match.length) {
					return true;
				}
			} else {
				i = 0;
			}
			// see if we've passed the stop point:
			if (!withinBlock && i == 0 && fsin.getPos() >= end) {
				return false;
			}
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
