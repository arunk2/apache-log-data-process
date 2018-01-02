package com.ppltech.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserExtractorReducer extends
		Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		NullWritable nullkey = NullWritable.get();
		Text session = new Text();

		for (Text val : values) {
			
			String[] valsplit = val.toString().split("#");
			session.set(valsplit[1]);
			key.set(key.toString()+" : "+valsplit[0]);
			context.write(nullkey, session);
			
		}
		
	}

}
