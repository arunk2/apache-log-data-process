package com.ppltech.prepross.extract;

import java.io.Serializable;

public class Video implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public String videoId;
	public String showId;
	public String showName;
	public String categoryId;
	public String categoryName;
	public String tagName;
	
	public Video(String entry) {
		String[] dtls = entry.split(",");
		videoId = dtls[0];
		showId = dtls[1];
		showName = dtls[2];
		categoryId = dtls[3];
		categoryName = dtls[4];
		tagName = dtls[5];
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return videoId+", "+
				showId+", "+
				showName+", "+
				categoryId+", "+
				categoryName+", "+
				tagName;
	}
}
