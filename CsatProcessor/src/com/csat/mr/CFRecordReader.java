package com.csat.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
/*import org.apache.poi.POIOLE2TextExtractor;
import org.apache.poi.extractor.ExtractorFactory;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;*/
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;

public class CFRecordReader extends RecordReader<Text, Text> {

	// private static final Logger LOG = Logger.getLogger(CFRecordReader.class);

	/** The path to the file to read. */
	private final Path mFileToRead;
	/** The length of this file. */
	private final long mFileLength;

	/** The Configuration. */
	private final Configuration mConf;

	/** Whether this FileSplit has been processed. */
	private boolean mProcessed;
	/** Single Text to store the file name of the current file. */
	// private final Text mFileName;
	/** Single Text to store the value of this file (the value) when it is read. */
	private final Text mFileText;
	// private Map<Long, String> mapParagraphs = new HashMap<Long, String>();
	private int smallFilesCount = 0;
	private Text key;
	private Text value;

	public CFRecordReader(CombineFileSplit split, TaskAttemptContext context,
			Integer index) throws IOException {

		mProcessed = false;
		mFileToRead = split.getPath(index);
		mFileLength = split.getLength(index);
		mConf = context.getConfiguration();

		// assert 0 == split.getOffset(index);
		System.out.println("FileToRead is: " + mFileToRead.toString());
		System.out.println("Processing path " + index + " out of "
				+ split.getNumPaths());

		// mFileName = new Text();
		mFileText = new Text();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		mFileText.clear();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
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
		smallFilesCount++;
		return (mProcessed) ? (float) 1.0 : (float) 0.0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		key = new Text();
		value = new Text();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!mProcessed) {
			if (mFileLength > (long) Integer.MAX_VALUE) {
				throw new IOException("File is longer than Integer.MAX_VALUE.");
			}
			// byte[] contents = new byte[(int) mFileLength];

			FileSystem fs = mFileToRead.getFileSystem(mConf);
			FSDataInputStream in = null;
			org.apache.poi.xwpf.extractor.XWPFWordExtractor oleTextExtractor = null;
			try {
				// Set the contents of this file.
				in = fs.open(mFileToRead);
				// IOUtils.readFully(in, contents, 0, contents.length);
				// mFileText.set(contents, 0, contents.length);

				//POIFSFileSystem fileSystem = new POIFSFileSystem(in);
				// POIOLE2TextExtractor oleTextExtractor = null;
				oleTextExtractor = new XWPFWordExtractor(
						new XWPFDocument(in));
				//System.out.print(oleTextExtractor.getText());

				//WordExtractor wordExtractor = null;

				/*
				 * try { oleTextExtractor =
				 * ExtractorFactory.createExtractor(fileSystem); } catch
				 * (Exception e) { // TODO Auto-generated catch block
				 * e.printStackTrace(); } if (oleTextExtractor instanceof
				 * WordExtractor) { wordExtractor = (WordExtractor)
				 * oleTextExtractor; }
				 */

				// mapParagraphs
				String docText = oleTextExtractor.getText();
				key.set(String.valueOf(smallFilesCount));
				value.set(docText);

			} finally {
				IOUtils.closeStream(in);
				oleTextExtractor.close();
			}
			mProcessed = true;
			return true;
		}
		return false;
	}

}
