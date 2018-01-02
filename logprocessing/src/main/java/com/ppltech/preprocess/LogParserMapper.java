package com.ppltech.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogParserMapper extends
		Mapper<Object, Text, Text, UserTransaction> {

	@Override
	public void setup(Context context) throws IOException, InterruptedException {

	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		Text sessionId = new Text();
		UserTransaction userTxn;
		
		try {

			userTxn = new UserTransaction(value.toString());
			
			String session = null;
			if (userTxn.getIsUser())
				session = userTxn.getUser().sessionId;
			else if (userTxn.getStatus() != null) 
				session = userTxn.getStatus().sessionId;
			else 
				return; //Process transaction, which we are interested in
			
			sessionId.set(session);
			
			
			if (userTxn.getIsUser() || (!userTxn.getIsUser() && userTxn.getStatus() != null)) {
				
				if(sessionId.getLength() > 1) {
					
					if(userTxn.getIsUser()) {
//						if(userTxn.getUser().playerFrom.equals("9")
//								|| (userTxn.getUser().playerFrom.equals("13"))
//								|| (userTxn.getUser().playerFrom.equals("22"))) {
							context.write(sessionId, userTxn);
//						}
					}
					else {
						context.write(sessionId, userTxn);
					}
				}
				else
					;//Invalid session details - Ignore this entry
			}
			
		} catch (Exception e) {
			throw new RuntimeException("Could not parse post", e);
		}
	}

}
