package com.ppltech.postprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PointToClusterReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Text session = new Text();

		Integer count = 0;
		for (Text val : values) {
			session.set(val);
			context.write(key, session);
			count++;
		}
//		session.set(count.toString());
//		context.write(key, session);
		
	}

}
