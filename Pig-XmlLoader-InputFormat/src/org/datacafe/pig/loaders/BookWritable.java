package org.datacafe.pig.loaders;

import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BookWritable {
	private Text bookTitle;
	private Text bookAuthor;
	private Text bookISBN;

	//boolean compareTo (object o1, object o2)
	//{
	//	return !(o1 > o2);
	//}
	
	public Text getBookTitle() {
		return bookTitle;
	}

	public void setBookTitle(Text bookTitle) {
		this.bookTitle = bookTitle;
	}

	public Text getBookAuthor() {
		return bookAuthor;
	}

	public void setBookAuthor(Text bookAuthor) {
		this.bookAuthor = bookAuthor;
	}

	public Text getBookISBN() {
		return bookISBN;
	}

	public void setBookISBN(Text bookISBN) {
		this.bookISBN = bookISBN;
	}

	public BookWritable(Text bookTitle, Text bookAuthor, Text bookISBN) {
		this.bookTitle = bookTitle;
		this.bookAuthor = bookAuthor;
		this.bookISBN = bookISBN;
	}
	public BookWritable() {
		this.bookTitle = new Text();
		this.bookAuthor = new Text();
		this.bookISBN = new Text();
	}
	public void write(DataOutput dataOutput) throws IOException {
		bookTitle.write(dataOutput);
		bookAuthor.write(dataOutput);
		bookISBN.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
    	bookTitle.readFields(dataInput);
        bookAuthor.readFields(dataInput);
        bookISBN.readFields(dataInput);
    }
    
    @Override
    public int hashCode() {
        // This is used by HashPartitioner, so implement it as per need
        // this one shall hash based on request id
        return bookTitle.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format(bookAuthor.toString() + " ****** " + bookTitle.toString() + " ****** " + bookISBN.toString());
    }

}
