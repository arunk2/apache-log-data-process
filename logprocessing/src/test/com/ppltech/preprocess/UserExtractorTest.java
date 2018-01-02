package com.ppltech.preprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.ppltech.preprocess.LogParserMapper;
import com.ppltech.preprocess.LogParserReducer;
import com.ppltech.preprocess.UserExtractorMapper;
import com.ppltech.preprocess.UserExtractorReducer;
import com.ppltech.preprocess.UserTransaction;
import com.ppltech.prepross.extract.SessionDetails;

public class UserExtractorTest {
	MapDriver<Text, SessionDetails, Text, Text> mapDriver;
	ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;
	MapReduceDriver<Text, SessionDetails, Text, Text, NullWritable, Text> mapReduceDriver;

	@Before
	public void setUp() {
		UserExtractorMapper mapper = new UserExtractorMapper();
		UserExtractorReducer reducer = new UserExtractorReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		
		String sessionId = "44c5f50e-95c1-48a4-98fb-0f4279c249f8";
		SessionDetails session = loadSession(sessionId);
		
		String viewerId = "1.39.15.185--1457966462";
		String logOutput = sessionId +"#"+ "1.39.15.185--1457966462,44c5f50e-95c1-48a4-98fb-0f4279c249f8,09:23,,1,0,764258,News,killed Murdersuicide Texas base Sheriff,1 0 0 0 0 1 0,3797:12610:1 4 0 0 0 0 0,";
		
		mapDriver.withInput(new Text(sessionId), session);
		mapDriver.withOutput(new Text(viewerId), new Text(logOutput));
		
		mapDriver.runTest();
		
	}

	private SessionDetails loadSession(String sessionId) throws IOException {
		
		//String sessionId = "44c5f50e-95c1-48a4-98fb-0f4279c249f8";
		MapReduceDriver<Object, Text, Text, UserTransaction, Text, SessionDetails> logDriver;
		LogParserMapper mapper = new LogParserMapper();
		LogParserReducer reducer = new LogParserReducer();
		logDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
		String entry = "{ \"time\": \"10/Apr/2016:00:00:00 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/userinfo.gif?q=44c5f50e-95c1-48a4-98fb-0f4279c249f8%23%3A%231.39.86.171%23%3A%232047%23%3A%23IN%23%3A%23764258%23%3A%232016-04-09%3A23%3A59%3A53%23%3A%23GJ%23%3A%231.39.15.185--1457966462%23%3A%230%23%3A%2311%23%3A%231%23%3A%23Chrome+Mobile+33.0%23%3A%23Android+4.4+KitKat%23%3A%23450%23%3A%23325%23%3A%23Adsession%3A12610%2Cpreroll%2C0%7C12611%2Cpostroll%2C0%23%3A%23true%23%3A%23false%23%3A%230.1%23%3A%23%23%3A%230%23%3A%23289%23%3A%23Smart+Phone%23%3A%230%23%3A%23 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.withInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:10 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"31\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/istatus.php?type=adstart&status=start&sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8&cmpro=3797&cmpid=12610&pid=2047&cid=IN&vid=764258&pfid=2&sc=2016-04-09%3A23%3A59%3A53&ptm=2&mvt=NA&vpu=-1&mvtro=NA&vpuro=0&cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:21 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=first;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:23 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=mid;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:29 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=third;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:35 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=complete;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:35 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=videoengengagement;status=waiting;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;vid=764258;vrid=62023836;pid=2047;cid=IN;cpid=659;ptm=html5 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:37 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=videostart;status=start;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;vid=764258;vrid=62023836;pid=2047;cid=IN;cpid=659 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		logDriver.addInput(new LongWritable(), new Text(entry));
		
		List<Pair<Text, SessionDetails>> res = logDriver.run();
		return res.get(0).getSecond();
		
	}
	
	@Test
	public void testReducer() throws IOException {		
		
		String sessionId = "44c5f50e-95c1-48a4-98fb-0f4279c249f8";
		String viewerId = "1.39.15.185--1457966462";
		
		String logOutput = "1.39.15.185--1457966462,44c5f50e-95c1-48a4-98fb-0f4279c249f8,09:23,,1,0,764258,News,killed Murdersuicide Texas base Sheriff,1 0 0 0 0 1 0,3797:12610:1 4 0 0 0 0 0,";
		
		List<Text> values = new ArrayList<Text>();
		values.add(new Text(sessionId +"#"+ logOutput));
		
		reduceDriver.withInput(new Text(viewerId), values);
		reduceDriver.withOutput(NullWritable.get(), new Text(logOutput));
		
		reduceDriver.runTest();
		
	}
	
	@Test
	public void testMapReduce() throws IOException {
		
		String sessionId = "44c5f50e-95c1-48a4-98fb-0f4279c249f8";
		SessionDetails session = loadSession(sessionId);
		
		String logOutput = "1.39.15.185--1457966462,44c5f50e-95c1-48a4-98fb-0f4279c249f8,09:23,,1,0,764258,News,killed Murdersuicide Texas base Sheriff,1 0 0 0 0 1 0,3797:12610:1 4 0 0 0 0 0,";
		
		mapReduceDriver.withInput(new Text(sessionId), session);
		mapReduceDriver.withOutput(NullWritable.get(), new Text(logOutput));
		
		mapReduceDriver.runTest();
		
	}
	
}
