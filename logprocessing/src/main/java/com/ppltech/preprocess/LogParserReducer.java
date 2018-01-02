package com.ppltech.preprocess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.ppltech.prepross.extract.SessionDetails;
import com.ppltech.prepross.extract.UserInfo;
import com.ppltech.prepross.extract.Video;
import com.ppltech.prepross.extract.VideoEvents;

public class LogParserReducer extends
		Reducer<Text, UserTransaction, Text, SessionDetails> {
	
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
	protected void reduce(Text key, Iterable<UserTransaction> values,
			Context context) throws IOException, InterruptedException {
		
		SessionDetails session = new SessionDetails();
		UserInfo user = new UserInfo();
		
		HashMap<String, String> map = new HashMap<String, String>();

		int count = 0;
		for (UserTransaction val : values) {
			
			count++;
			
			if (val.getIsUser()) {
				
				user = val.getUser();
				
				
			} else {
				String events = null;
				String statusType = val.getStatus().type;
				/* Video events */
				if (statusType.equals(VideoEvents.VIDEO_START)
						|| (statusType.equals(VideoEvents.VIDEO_ENGAGEMENT))
						|| (statusType.equals(VideoEvents.VIDEO_PROGRESS))) {

					statusType = val.getStatus().status + ":" + statusType;

					events = map.get("v-" + val.getStatus().vId);
					if (events == null)
						events = statusType;
					else
						events = events + "," + statusType;

					map.put("v-" + val.getStatus().vId, events);

				}
				/* Advertisement events */
				else if (statusType.equals(VideoEvents.AD_START)
						|| (statusType.equals(VideoEvents.AD_ENGAGEMENT))
						|| (statusType.equals(VideoEvents.AD_PROGRESS))
						|| (statusType.equals(VideoEvents.AD_CLICK))) {

					statusType = val.getStatus().status + ":" + statusType;

					events = map.get(val.getStatus().cmpro + "-"
							+ val.getStatus().cmpId);
					if (events == null)
						events = statusType;
					else
						events = events + "," + statusType;

					map.put(val.getStatus().cmpro + "-"
							+ val.getStatus().cmpId, events);

				} else {
					// Discard other events
				}
			}
		}
		if (count > 1) {
			
			loadSession(map, session, user, key.toString());
			if(cache.containsKey(session.video.videoId)) {
				session.video.category = cache.get(session.video.videoId).categoryName;
				session.video.tags = cache.get(session.video.videoId).tagName;
			}
			
			if (session.viewerId.length() > 1)
				context.write(key, session);
		}
		else {
			//Don't write anything. There are no details
		}
		
	}

	private void loadSession(HashMap<String, String> events, SessionDetails session, UserInfo user, String sessionId) {
		session.loadEvents(user, events, sessionId);
	}
}
