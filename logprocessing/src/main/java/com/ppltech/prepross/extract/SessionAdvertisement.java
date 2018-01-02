package com.ppltech.prepross.extract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SessionAdvertisement {
	
	public String campId="";
	public String campRo="";
	
	public VideoEvents aEvents = new VideoEvents();
	
	public void loadEvent(String events) {
		aEvents.loadEvent(events);
	}
	
	@Override
	public String toString() {
		return campRo + ":" +
				campId + ":" +
				aEvents.toString();
	}

	public void readFields(DataInput in) throws IOException {
		campRo = in.readUTF();
		campId = in.readUTF();
		aEvents.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(campRo);
		out.writeUTF(campId);
		aEvents.write(out);
	}
	
}
