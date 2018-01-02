package com.ppltech.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class ClusterViewer {

	private static String USER_PATH = "target/output-base/users/part-r-00000";
	private static String CLUSTER_PATH = "target/output-base/clusters/clusterOutPath/part-r-00000";
	
	private Map<String, String> userSession = new HashMap<String, String>();

	public static void main(String[] args) throws Exception {

		ClusterViewer viewer = new ClusterViewer();

		viewer.loadCache();
		viewer.printCluster();

	}

	private void printCluster() throws Exception {
		String curLine;
		BufferedReader cluster_br = new BufferedReader(new FileReader(CLUSTER_PATH));
		
		Map<String, Integer> dictionary = new HashMap<String, Integer>();
		
		//For the first line - cluster seperation
		curLine = cluster_br.readLine();
		String [] str = curLine.split("\t"); 
//		System.out.print(str[0]);
		String userDetails = userSession.get(str[1]);
//		System.out.println(" "+userDetails);
		String prevClusterId = str[0];
		String curClusterId = str[0];
		loadKeywords(dictionary, userDetails);
		int count = 0;
		
		// Load the user session map
		while ((curLine = cluster_br.readLine()) != null) {
			count++;
			str = curLine.split("\t"); 
			curClusterId = str[0];
			
			if(curClusterId.equals(prevClusterId)) {
				//Ignore as it is in same cluster
			}
			else {
				String[] keywords = getTopKeywords(dictionary, 10);
				System.out.println("End of cluster : "+curClusterId+" Count = "+count);
				for(String key : keywords) 
					System.out.print(key+ " ");
				System.out.println();
				System.out.println();
				prevClusterId = curClusterId;
				dictionary.clear();
				count = 0;
			}
			
//			System.out.print(str[0]);
			userDetails = userSession.get(str[1]);
//			System.out.println(" "+ userDetails);
			
			loadKeywords(dictionary, userDetails);
			
		}
		cluster_br.close();
		String[] keywords = getTopKeywords(dictionary, 10);
		System.out.println("End of cluster : "+curClusterId+" Count = "+count);
		for(String key : keywords) 
			System.out.print(key+ " ");
		
	}

	private void loadKeywords(Map<String, Integer> dictionary,
			String userDetails) {
		
		String[] keywords = userDetails.split(" ");
		for(String word : keywords) {
			if(word.length() > 1 && !word.equals(" ")) {
				if(dictionary.containsKey(word)) {
					dictionary.put(word, (dictionary.get(word)+1));
				}
				else {
					dictionary.put(word, 1);
				}
			}
		}
	}
	
	private String[] getTopKeywords(Map<String, Integer> dictionary, int k) {
		
		String[] keywords = new String[k];
		MyComparator bvc =  new MyComparator(dictionary);
        TreeMap<String,Integer> sorted_map = new TreeMap<String,Integer>(bvc);
        sorted_map.putAll(dictionary);
        
        int i = 0;
        for(Entry<String, Integer> entry : sorted_map.entrySet()) {
        	keywords[i] = entry.getKey(); ////+" : "+entry.getValue()+"    ";
        	i++;
        	if (i >= k) break;
        }
		return keywords;
		
	}

	private void loadCache() throws Exception {
		String curLine;
		BufferedReader users_br = new BufferedReader(new FileReader(USER_PATH));
		
		//Load the user session map
		while ((curLine = users_br.readLine()) != null) {
			
			String [] str = curLine.split(","); 
			String key = str[0]+":"+str[1];
			String value = "";
			if(str.length > 7)
				value = str[7]+" "+str[8];
			
			//Append add details
//			if(str.length > 9) {
//				for (int i = 9; i < str.length; i++) {
//					value = value + "," + str[i];
//				}
//			}
			userSession.put(key, value);
			
		}
		users_br.close();
	}
	
	
}

class MyComparator implements Comparator<String> {

    Map<String, Integer> base;
    public MyComparator(Map<String, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
