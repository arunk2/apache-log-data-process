package com.ppltech.prepross.extract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SessionVideo {
	
	public String videoId="";
	public String category="";
	public String tags="";
	
	public VideoEvents vEvents = new VideoEvents();

	public void loadEvent(String events) {
		vEvents.loadEvent(events);
	}
	
	@Override
	public String toString() {
		String spacedTags = tags.replaceAll(";", " ");
		return 
				videoId + "," +
				category + "," +
				spacedTags + "," + 
				vEvents.toString();
	}

	public void readFields(DataInput in) throws IOException {
		videoId = in.readUTF();
		category = in.readUTF();
		tags = in.readUTF();
		vEvents.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(videoId);
		out.writeUTF(category);
		out.writeUTF(tags);
		vEvents.write(out);
	}
	
}
