package com.ppltech.prepross.extract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VideoEvents {
	
	public static String AD_START = "adstart";
	public static String AD_ENGAGEMENT = "adengagement";
	public static String AD_PROGRESS = "adprogress";
	public static String AD_CLICK = "adclick";
	public static String VIDEO_START = "videostart";
	public static String VIDEO_ENGAGEMENT = "videoengengagement";
	public static String VIDEO_PROGRESS = "videoprogress";
	
	
	public Integer start = 0;
	public Integer progress = 0;

	public Integer engagementPause = 0;
	public Integer engagementMute = 0;
	public Integer engagementAbort = 0;
	public Integer engagementOthers = 0;
	
	public Integer click = 0;
	
	public void loadEvent(String events) {
		String[] vEvents = events.split(",");
		
		for(String vEvent : vEvents) {
			
			if(vEvent.endsWith("start")) {
				this.start = 1;
			}
			else if (vEvent.endsWith("progress")) {
				this.progress++;
			}
			else if (vEvent.endsWith("gagement")) {
				String[] status = vEvent.split(":");
				if(status[0].equals("mute"))
					engagementMute = 1;
				else if(status[0].equals("pause"))
					engagementPause = 1;
				else if(status[0].equals("abort"))
					engagementAbort = 1;
				else
					engagementOthers++;
			}
			else if (vEvent.endsWith("click")) {
				this.click++;
			}
		}
	}
	
	@Override
	public String toString() {
		return 
				String.valueOf(start)+" "+
				String.valueOf(progress)+" "+
				String.valueOf(engagementAbort)+" "+
				String.valueOf(engagementMute)+" "+
				String.valueOf(engagementPause)+" "+
				String.valueOf(engagementOthers)+" "+
				String.valueOf(click);
	}

	public void readFields(DataInput in) throws IOException {
		start = in.readInt();
		progress = in.readInt();
		engagementAbort = in.readInt();
		engagementMute = in.readInt();
		engagementPause = in.readInt();
		engagementOthers = in.readInt();
		click = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(start);
		out.writeInt(progress);
		out.writeInt(engagementAbort);
		out.writeInt(engagementMute);
		out.writeInt(engagementPause);
		out.writeInt(engagementOthers);
		out.writeInt(click);
	}
	
}
