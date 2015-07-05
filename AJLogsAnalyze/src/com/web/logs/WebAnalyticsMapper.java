package com.web.logs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebAnalyticsMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	WeblogsData weblogsData = null;
	private Map<String, String> ipCountryMap = new HashMap<String, String>();
	private Text outputKey = new Text();
	//private WeblogsData outputValue = new WeblogsData();
	private final static IntWritable one = new IntWritable(1);

	public static String getNextIPV4Address(String ip) {

		String[] nums = ip.split("\\.");
		int i = (Integer.parseInt(nums[0].trim()) << 24
		| Integer.parseInt(nums[2].trim()) << 8
		| Integer.parseInt(nums[1].trim()) << 16 | Integer
		.parseInt(nums[3].trim())) + 1;
		return String.format("%d.%d.%d.%d", i >>> 24 & 0xFF, i >> 16 & 0xFF,
		i >> 8 & 0xFF, i >> 0 & 0xFF);
	}

	protected void setup(Context context) throws IOException, InterruptedException {

		String[] ips = context.getConfiguration().get("ip_data").split("@@@");
		for (int i = 0; i < ips.length; i++) {
			System.out.println(ips[i].toString());
			if(!(ips[i].equals(""))){
				String[] tokens = ips[i].split(",");
				System.out.println("tokens length" + tokens.length);
				String startIP = tokens[0];
				String endIP = tokens[1];
				String country_name = tokens[4].trim();
				if (country_name.replaceAll("\\s+","").equalsIgnoreCase("India")) {
					ipCountryMap.put(startIP, country_name);
					while (!startIP.trim().equals(endIP.trim())) {
						startIP = getNextIPV4Address(startIP);
						ipCountryMap.put(startIP, country_name);
					}
				}
			}
		}
		if (ipCountryMap.isEmpty()) {
			throw new IOException("Unable to load IP country table.");
		}
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		if(ivalue.toString().contains("http://mockus.in/")){
			String[] ipString =  ivalue.toString().split(" ",2);
			// retrieving Ip address from a weblog entry 
			String ip = ipString[0];
			/*String[] subString = ipString[1].split(":",2);
			// retrieving date value from a weblog entry
			String date = subString[0].split("- -",2)[1].substring(2);
			weblogsData = new WeblogsData();
			String country = ipCountryMap.get(ip);
			String countryValue = country != null ? country:"others";
			//Date.parse(date);
			weblgsData.setDate(new Text(date));
			weblogsData.setIp(new Text(ip));
			weblogsData.setWebVisit((long) 1);
			weblogsData.setCountry(new Text(countryValue));
			outputKey.set(date);
			outputValue = weblogsData;*/
			String country = ipCountryMap.get(ip);
			String countryValue = country != null ? country:"India";
			outputKey.set(countryValue);
		}
		context.write(outputKey, one);
	}
}
