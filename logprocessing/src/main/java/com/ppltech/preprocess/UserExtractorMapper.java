package com.ppltech.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ppltech.prepross.extract.SessionDetails;

public class UserExtractorMapper extends
		Mapper<Text, SessionDetails, Text, Text> {
	
	Text viewerId = new Text();
	Text session = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	public void map(Text key, SessionDetails value, Context context)
			throws IOException, InterruptedException {
		try {
			
			session.set(key.toString()+ "#" +value.toString());
			viewerId.set(value.viewerId);
			context.write(viewerId, session);
			
		} catch (Exception e) {
			throw new RuntimeException("Could not parse post", e);
		}
	}

}
