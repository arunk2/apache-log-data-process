package com.ppltech.prepross.extract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class SessionDetails implements Writable {

	public String viewerId="";
	public String sessionId="";
	public String dateTime="";

	public String pageURL="";
	public String playerPos="";
	public String playerSize="";

	public SessionVideo video  = new SessionVideo();
	public Integer adsCount = 0;
	public List<SessionAdvertisement> advertisements = new ArrayList<SessionAdvertisement>();
	

	public void loadEvents(UserInfo user, HashMap<String, String> events, String session) {
		this.sessionId = session;
		this.viewerId = user.viewerId;
		this.dateTime = user.date;
		
		try {
			this.pageURL = URLDecoder.decode(user.pageURL);
		}
		catch(Exception e) {
			System.out.println("ERROR: URLDecoder issue : "+user.pageURL);
			this.pageURL = "";
		}
		
		this.playerPos = user.position;
		this.playerSize = user.playerSize;

		video.videoId = user.videoId;
		video.category = "";
		video.tags = "";

		for (Entry<String, String> entry : events.entrySet()) {

			if (entry.getKey().startsWith("v")) {
				// Load video events
				video.loadEvent(entry.getValue());

			} else {
				// Load ad events
				SessionAdvertisement ad = advertisementsGet(entry.getKey());
				
				if (ad == null) {
					
					adsCount++;  //Ads count
					String[] keys = entry.getKey().split("-");
					SessionAdvertisement newAd = new SessionAdvertisement();
					newAd.campId = keys[1];
					newAd.campRo = keys[0];
					newAd.loadEvent(entry.getValue());
					
					advertisementsPut(entry.getKey(), newAd);
					
				} else {
					
					ad.loadEvent(entry.getValue());
					
				}
			}
			
		}

	}

	private void advertisementsPut(String key, SessionAdvertisement newAd) {
		advertisements.add(newAd);
	}

	private SessionAdvertisement advertisementsGet(String key) {
		for(int i = 0; i < adsCount; i++) {
			if(key.startsWith(advertisements.get(i).campRo))
				return advertisements.get(i);
		}
		return null;
	}

	@Override
	public String toString() {
		String ret = 
				viewerId + "," +
				sessionId + "," +
				dateTime.substring(8, 13) + "," +
				pageURL + "," +
				playerPos + "," +
				playerSize + "," +
				video.toString();
				
		if (advertisements.size() > 0) {
			ret += ",";
			for (SessionAdvertisement ad : advertisements) {
				ret += ad.toString() + ",";
			}
			ret += "";
		}
		
		return ret;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		viewerId = in.readUTF();
		sessionId = in.readUTF();
		dateTime = in.readUTF();
		pageURL = in.readUTF();
		playerPos = in.readUTF();
		playerSize = in.readUTF();
		video.readFields(in);
		adsCount = in.readInt();
		if (adsCount > 0)
			advertisements = new ArrayList<SessionAdvertisement>();
		
		for(int i = 0; i < adsCount; i++) {
			SessionAdvertisement adv = new SessionAdvertisement();
			adv.readFields(in);
			advertisements.add(adv);
		}
		
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(viewerId);
		out.writeUTF(sessionId);
		out.writeUTF(dateTime);
		out.writeUTF(pageURL);
		out.writeUTF(playerPos);
		out.writeUTF(playerSize);
		
		video.write(out);
		out.writeInt(adsCount);
		
		for(int i = 0; i < adsCount; i++) {
			advertisements.get(i).write(out);
		}
	}

}
