package com.web.logs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WeblogsData implements Writable{
	
	private Text date;
	private Text ip;
	private long webVisit=0;
	private Text country;
	
	public Text getDate() {
		return date;
	}
	public void setDate(Text date) {
		this.date = date;
	}
	public Text getIp() {
		return ip;
	}
	public void setIp(Text ip) {
		this.ip = ip;
	}
	public Long getWebVisit() {
		return webVisit;
	}
	public void setWebVisit(Long webVisit) {
		this.webVisit = webVisit;
	}
	public Text getCountry() {
		return country;
	}
	public void setCountry(Text country) {
		this.country = country;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		date = new Text(in.toString());
		ip = new Text(in.toString());
		webVisit = in.readLong();
		country = new Text(in.toString());
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeBytes(date.toString());
		out.writeBytes(ip.toString());
		out.writeLong(webVisit);
		out.writeBytes(country.toString());
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return date.toString()+ "," + ip.toString()+ "," + webVisit + "," 
				+ country.toString();
	}
}

