package com.ppltech.cluster;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ppltech.prepross.extract.Video;

public class VectorizeMapper extends
		Mapper<Object, Text, Text, Text> {

	HashMap<String, Video> cache = new HashMap<String, Video>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		loadVideoDetails();
	}
	
	private void loadVideoDetails() {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(
					"src/main/resources/video_master.csv"));

			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				Video videoEntry = new Video(sCurrentLine);
				cache.put(videoEntry.videoId, videoEntry);

			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		Text viewerSession = new Text();
		Text keywords = new Text();
		
		//Trim the value to retain only keywords
		String [] vals = value.toString().split(","); 
		if(vals.length > 1) {
			viewerSession.set(vals[0]+":"+vals[1]);
		}
		else
			return; //Ignore input
		
		if(vals.length > 7)  {
//			keywords.set(vals[7]+" "+vals[8]); 
			if(cache.containsKey(vals[6])) {
				keywords.set(cache.get(vals[6]).categoryName + " " + cache.get(vals[6]).tagName.replaceAll(";", " "));
			}
			else {
				keywords.set(""); 
			}
			
		}
		else
			keywords.set("");
		
		context.write(viewerSession, keywords);
		
	}
	
}
