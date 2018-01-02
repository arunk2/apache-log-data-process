package com.ppltech.prepross.extract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StatusInfo implements Writable {
	
	public String sessionId;
	public String type;
	public String status;
	public String cmpro;
	public String cmpId;
	public String publisherId;
	public String cId;
	public String vId;
	public String pfId; //Player from Id - like editorial, content, template, etc...
	public String sc;
	public String vrId;
	public String cpId;
	
	@Override
	public String toString() {
		return 
		sessionId+", "+
		type+", "+
		status+", "+
		cmpro+", "+
		cmpId+", "+
		publisherId+", "+
		cId+", "+
		vId+", "+
		pfId+", "+
		sc;
	}
	
	public StatusInfo() {
	}
	
	public StatusInfo(String request) {
		
		try {
			String[] req = request.split("type=");

			String[] dtls = req[1].split(";");
			type = dtls[0];
			status = dtls[1].replace("status=", "");
			sessionId = dtls[2].replace("sid=", "");
			cmpro = dtls[3].replace("cmpro=", "");
			cmpId = dtls[4].replace("cmpid=", "");
			publisherId = dtls[5].replace("pid=", "");
			cId = dtls[6].replace("cid=", "");
			vId = dtls[7].replace("vid=", "");
			// pfId = dtls[8].replace("pfid=", "");
			// sc = dtls[9].replace("sc=", "");
		} catch (Exception e) {
			System.out.println("WARNING: StatusInfo - Problem with input data : " + request);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		sessionId = in.readUTF();
		type = in.readUTF();
		status = in.readUTF();
		cmpro = in.readUTF();
		cmpId = in.readUTF();
		publisherId = in.readUTF();
		cId = in.readUTF();
		vId = in.readUTF();
//		out.writeUTF(sc);
//		pfId = in.readUTF();
//		sc = in.readUTF();
//		vrId = in.readUTF();
//		cpId = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(sessionId);
		out.writeUTF(type);
		out.writeUTF(status);
		out.writeUTF(cmpro);
		out.writeUTF(cmpId);
		out.writeUTF(publisherId);
		out.writeUTF(cId);
		out.writeUTF(vId);
//		out.writeUTF(pfId);
//		out.writeUTF(sc);
//		out.writeUTF(vrId);
//		out.writeUTF(cpId);
		
	}
}
