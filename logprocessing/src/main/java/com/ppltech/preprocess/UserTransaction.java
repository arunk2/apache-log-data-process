package com.ppltech.preprocess;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;
import com.ppltech.prepross.extract.LogEntry;
import com.ppltech.prepross.extract.StatusInfo;
import com.ppltech.prepross.extract.UserInfo;

public class UserTransaction implements Writable {

	private UserInfo user = new UserInfo();
	private StatusInfo status = new StatusInfo();
	private Boolean isUser = false;
	
	public UserInfo getUser() {
		return user;
	}

	public void setUser(UserInfo user) {
		this.user = user;
	}

	public StatusInfo getStatus() {
		return status;
	}

	public void setStatus(StatusInfo status) {
		this.status = status;
	}

	public Boolean getIsUser() {
		return isUser;
	}

	public void setIsUser(Boolean isUser) {
		this.isUser = isUser;
	}
	
	public UserTransaction() {
		
	}

	public UserTransaction(String logEntry) throws UnsupportedEncodingException {
		Gson gson = new Gson();
		// convert the json string back to request details
		LogEntry entry = gson.fromJson(logEntry, LogEntry.class);
		loadDetails(entry);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isUser);
		if (isUser)
			user.write(out);
		else
			status.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isUser = in.readBoolean();
		if (isUser)
			this.user.readFields(in);
		else
			this.status.readFields(in);
	}
	
	private void loadDetails(LogEntry entry)
			throws UnsupportedEncodingException {
		
		String request = "";
		try {
			request = URLDecoder.decode(entry.getRequest(), "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		request = request.replace(" HTTP/1.1", "");

		setIsUser(false);
		
		if (request.contains("status=loadtime")) {
			//Clear loadtime events - as it is only for tracking purpose
			//It is not an user engagement
			setUser(null);
			setStatus(null);
		}
		else if (request.contains("userinfo.gif")) {
			// Load user request details
			UserInfo user = new UserInfo(request);
			setUser(user);
			setIsUser(true);	
		}
		else if (request.contains("status.gif") || request.contains("istatus.php")) {
			StatusInfo status = null;
			if (request.contains("type=adstart")
					|| request.contains("type=adengagement;")
					|| request.contains("type=adprogress")) {
				request = request.replaceAll("&", ";"); //To avoid inconsistency
				status = new StatusInfo(request);
			}
			else if (request.contains("type=videostart")
					|| request.contains("type=videoengengagement")
					|| request.contains("type=videoprogress")) {
				status = new StatusInfo(request);
			}
			else {
				//Ignore other status
			}
			setStatus(status);
		}
		else {
			//Clear for other request types
			setUser(null);
			setStatus(null);
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		UserTransaction txn = (UserTransaction) obj;
		if(this.isUser == txn.isUser) {
			if(this.isUser) {
				if(this.user.sessionId.equals(txn.user.sessionId))
					return true;
			}
			else {
				if(this.status.sessionId.equals(txn.status.sessionId))
					return true;
			}
		}
		
		return false;
	}
	
	@Override
	public int hashCode() {
		
		if (this.isUser)
			return this.user.sessionId.hashCode();
		else
			return this.status.sessionId.hashCode();

	}

}
