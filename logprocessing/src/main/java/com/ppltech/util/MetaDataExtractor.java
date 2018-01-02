package com.ppltech.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class MetaDataExtractor {
	
	private List<String> urls = new ArrayList<String>();
	private Map<String, String> urlKeywords = new HashMap<String, String>();

	public static void main(String[] args) throws IOException {
		
		MetaDataExtractor extractor = new MetaDataExtractor();
		String sCurrentLine = null;
		BufferedReader br = new BufferedReader(new FileReader("src/resources/logdata.csv"));
		br.readLine(); //Ignore first line
		while ((sCurrentLine = br.readLine()) != null) {
			String[] str = sCurrentLine.split(",");
			if(str.length > 2)
				extractor.urls.add(URLDecoder.decode(str[3]));
		}
		br.close();
		
		extractor.process();
		extractor.output();
		
	}
	
	private void output() throws IOException {
		
		File file = new File("src/resources/keywords.csv");
		FileWriter fileWriter = new FileWriter(file);
		for(java.util.Map.Entry<String, String> entry : urlKeywords.entrySet()) {
			fileWriter.write(entry.getKey()+":"+entry.getValue()+"\n");
		}
		fileWriter.close();
		
	}

	private String getKeywords(String url) throws IOException {
		
		try {
			Document doc = Jsoup.connect(url).get();
			
			Elements elements = doc.select("meta");
			String keywords = "";
			for(int i = 0; i < elements.size(); i++) {
				Element ele = elements.get(i);
				if (ele.toString().toLowerCase().contains("keyword")) {
					keywords = ele.attr("content");
				}
			}
			return keywords;
		}
		catch(Exception e) {
			System.out.println("--------------------------"+url+"--------------------------");
			e.printStackTrace();
		}
		return "";
		
	}

	public void process() throws IOException {
		
		for(String url : urls) {
			//Avoid duplicate crawling
			if ( !urlKeywords.containsKey(url) ) {
				String keywords = getKeywords(url);
				keywords = keywords.replaceAll("\n", " ");
				urlKeywords.put(url, keywords);
			}
		}
		
	}

	public List<String> getUrls() {
		return urls;
	}

	public void setUrls(List<String> urls) {
		this.urls = urls;
	}
	
}
