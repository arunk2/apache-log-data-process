package com.ppltech.prepross.extract;

public class LogEntry {
	private String time;
	private String remote_addr;
	private String remote_user;
	private String body_bytes_sent;
	private String request_time;
	private String status;
	private String request;
	private String request_method;
	private String http_referrer;
	private String http_user_agent;
	
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getRemote_addr() {
		return remote_addr;
	}
	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}
	public String getRemote_user() {
		return remote_user;
	}
	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}
	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}
	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}
	public String getRequest_time() {
		return request_time;
	}
	public void setRequest_time(String request_time) {
		this.request_time = request_time;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getRequest_method() {
		return request_method;
	}
	public void setRequest_method(String request_method) {
		this.request_method = request_method;
	}
	public String getHttp_referrer() {
		return http_referrer;
	}
	public void setHttp_referrer(String http_referrer) {
		this.http_referrer = http_referrer;
	}
	public String getHttp_user_agent() {
		return http_user_agent;
	}
	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}
	
}
