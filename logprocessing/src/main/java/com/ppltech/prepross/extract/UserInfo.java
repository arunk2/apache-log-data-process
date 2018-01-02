package com.ppltech.prepross.extract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserInfo implements Writable {

	public String sessionId;
	public String ip;
	public String date;
	public String publisherId;
	public String videoId;
	public String cId;
	public String state;
	public String viewerId;
	public String affId;
	public String playerFrom;
	public String position;
	public String browser;
	public String os;
	public String autoStart;
	public String mute;
	public String volume;
	public String width;
	public String height;
	public String playerSize;
	public String pageURL;
	public String slotId;
	public String cityId;
	public String deviceType;
	public String userInitiated;
	public String adsession;

	@Override
	public String toString() {
		return sessionId + ", " + ip + ", " + date + ", " + publisherId + ", "
				+ videoId + ", " + cId + ", " + state + ", " + viewerId + ", "
				+ affId + ", " + playerFrom + ", " + position + ", " + browser
				+ ", " + os + ", " + autoStart + ", " + mute + ", " + volume
				+ ", " + width + ", " + height + ", " + playerSize + ", "
				+ pageURL + ", " + slotId + ", " + cityId + ", " + deviceType
				+ ", " + userInitiated + ", " + adsession;
	}
	
	public UserInfo() {
		sessionId = "";
		ip = "";
		date = "";
		publisherId = "";
		videoId = "";
		cId = "";
		state = "";
		viewerId = "";
		affId = "";
		playerFrom = "";
		position = "";
		browser = "";
		os = "";
		autoStart = "";
		mute = "";
		volume = "";
		width = "";
		height = "";
		playerSize = "";
		pageURL = "";
		slotId = "";
		cityId = "";
		deviceType = "";
		userInitiated = "";
		adsession = "";
	}

	public UserInfo(String request) {

		String[] req = request.split("q=");

		if (req.length <= 1) {
			sessionId = "";
			ip = "";
			date = "";
			publisherId = "";
			videoId = "";
			cId = "";
			state = "";
			viewerId = "";
			affId = "";
			playerFrom = "";
			position = "";
			browser = "";
			os = "";
			autoStart = "";
			mute = "";
			volume = "";
			width = "";
			height = "";
			playerSize = "";
			pageURL = "";
			slotId = "";
			cityId = "";
			deviceType = "";
			userInitiated = "";
			adsession = "";
		} else {
			try {
				String[] dtls = req[1].split("#:#");
				sessionId = dtls[0];
				ip = dtls[1];
				publisherId = dtls[2];
				cId = dtls[3];
				videoId = dtls[4];
				date = dtls[5];
				state = dtls[6];
				viewerId = dtls[7];
				affId = dtls[8];
				playerFrom = dtls[9];
				position = dtls[10];
				browser = dtls[11];
				os = dtls[12];
				width = dtls[13];
				height = dtls[14];
				playerSize = "0";
				// if(width // TODO Auto-generated constructor stubh > 640 &&
				// height
				// >
				// 480) {
				// player_size=2;
				// }
				// else if(width >= 290 && height >= 240) {
				// player_size=1;
				// }
				adsession = dtls[15].replace("Adsession:", "");
				autoStart = dtls[16];
				mute = dtls[17];
				volume = dtls[18];
				pageURL = dtls[19];

				if (dtls.length > 20)
					slotId = dtls[20];
				else
					slotId = "";
				if (dtls.length > 21)
					cityId = dtls[21];
				else
					cityId = "";
				if (dtls.length > 22)
					deviceType = dtls[22];
				else
					deviceType = "";
				if (dtls.length > 23)
					userInitiated = dtls[23];
				else
					userInitiated = "";
			} catch (Exception e) {
				System.out.println("WARNING: UserInfo - Problem with input data : " + request);
				sessionId = "";
				ip = "";
				date = "";
				publisherId = "";
				videoId = "";
				cId = "";
				state = "";
				viewerId = "";
				affId = "";
				playerFrom = "";
				position = "";
				browser = "";
				os = "";
				autoStart = "";
				mute = "";
				volume = "";
				width = "";
				height = "";
				playerSize = "";
				pageURL = "";
				slotId = "";
				cityId = "";
				deviceType = "";
				userInitiated = "";
				adsession = "";
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		sessionId = in.readUTF();
		ip = in.readUTF();
		date = in.readUTF();
		publisherId = in.readUTF();
		videoId = in.readUTF();
		cId = in.readUTF();
		state = in.readUTF();
		viewerId = in.readUTF();
		affId = in.readUTF();
		playerFrom = in.readUTF();
		position = in.readUTF();
		browser = in.readUTF();
		os = in.readUTF();
		autoStart = in.readUTF();
		mute = in.readUTF();
		volume = in.readUTF();
		width = in.readUTF();
		height = in.readUTF();
		playerSize = in.readUTF();
		pageURL = in.readUTF();
		slotId = in.readUTF();
		cityId = in.readUTF();
		deviceType = in.readUTF();
		userInitiated = in.readUTF();
		adsession = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(sessionId);
		out.writeUTF(ip);
		out.writeUTF(date);
		out.writeUTF(publisherId);
		out.writeUTF(videoId);
		out.writeUTF(cId);
		out.writeUTF(state);
		out.writeUTF(viewerId);
		out.writeUTF(affId);
		out.writeUTF(playerFrom);
		out.writeUTF(position);
		out.writeUTF(browser);
		out.writeUTF(os);
		out.writeUTF(autoStart);
		out.writeUTF(mute);
		out.writeUTF(volume);
		out.writeUTF(width);
		out.writeUTF(height);
		out.writeUTF(playerSize);
		out.writeUTF(pageURL);
		out.writeUTF(slotId);
		out.writeUTF(cityId);
		out.writeUTF(deviceType);
		out.writeUTF(userInitiated);
		out.writeUTF(adsession);
		
	}
}
