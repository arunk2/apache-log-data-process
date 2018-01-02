package com.ppltech.preprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ppltech.preprocess.LogParserMapper;
import com.ppltech.preprocess.LogParserReducer;
import com.ppltech.preprocess.UserTransaction;
import com.ppltech.prepross.extract.SessionDetails;

public class LogParserTest {
	MapDriver<Object, Text, Text, UserTransaction> mapDriver;
	ReduceDriver<Text, UserTransaction, Text, SessionDetails> reduceDriver;
	MapReduceDriver<Object, Text, Text, UserTransaction, Text, SessionDetails> mapReduceDriver;

	@Before
	public void setUp() {
		LogParserMapper mapper = new LogParserMapper();
		LogParserReducer reducer = new LogParserReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		String entry = "{ \"time\": \"10/Apr/2016:00:00:00 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"52.36.154.44\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/userinfo.gif?q=efa9dec4-c1d5-4a29-97ac-ba892cd57276%23%3A%23117.206.190.222%23%3A%231987%23%3A%23IN%23%3A%231%23%3A%232016-04-10%3A00%3A00%3A00%23%3A%23RJ%23%3A%23117.206.190.222--1460226599%23%3A%230%23%3A%232%23%3A%230%23%3A%23Chrome+Mobile+30.0%23%3A%23Android+4.4+KitKat%23%3A%23450%23%3A%23325%23%3A%23Adsession%3A12610%2Cpreroll%2C0%23%3A%231%23%3A%231%23%3A%230.2%23%3A%23all%23%3A%230%23%3A%2325774%23%3A%23Smart+Phone%23%3A%23 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"-\", \"http_user_agent\": \"-\" }";
		mapDriver.withInput(new LongWritable(), new Text(entry));
		UserTransaction txn = new UserTransaction(entry);
		mapDriver.withOutput(new Text("efa9dec4-c1d5-4a29-97ac-ba892cd57276"), txn);
		mapDriver.runTest();
		
		entry = "{ \"time\": \"10/Apr/2016:00:00:00 +0530\", \"remote_addr\": \"10.0.2.217\", \"client_ip\": \"101.0.40.149\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=third;sid=195d0557-124c-450c-b2eb-d621afca3af1;cmpro=3797;cmpid=12610;pid=1693;cid=IN;vid=764291;pfid=2;sc=2016-04-09%3A23%3A58%3A27;ptm=1;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://www.jagran.com/cricket/headlines-mumbai-indians-vs-pune-supergiants-13843623.html?src=p1\", \"http_user_agent\": \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537.36\" }";
		mapDriver.withInput(new LongWritable(), new Text(entry));
		UserTransaction txn1 = new UserTransaction(entry);
		mapDriver.withOutput(new Text("195d0557-124c-450c-b2eb-d621afca3af1"), txn1);
		mapDriver.runTest();
		
	}

	@Test
	public void testReducer() throws IOException {		
		String sessionId = "44c5f50e-95c1-48a4-98fb-0f4279c249f8";
		
		String entry = "{ \"time\": \"10/Apr/2016:00:00:00 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/userinfo.gif?q=44c5f50e-95c1-48a4-98fb-0f4279c249f8%23%3A%231.39.86.171%23%3A%232047%23%3A%23IN%23%3A%23764258%23%3A%232016-04-09%3A23%3A59%3A53%23%3A%23GJ%23%3A%231.39.15.185--1457966462%23%3A%230%23%3A%2311%23%3A%231%23%3A%23Chrome+Mobile+33.0%23%3A%23Android+4.4+KitKat%23%3A%23450%23%3A%23325%23%3A%23Adsession%3A12610%2Cpreroll%2C0%7C12611%2Cpostroll%2C0%23%3A%23true%23%3A%23false%23%3A%230.1%23%3A%23%23%3A%230%23%3A%23289%23%3A%23Smart+Phone%23%3A%230%23%3A%23 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn1 = new UserTransaction(entry);
		entry = "{ \"time\": \"10/Apr/2016:00:00:10 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"31\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/istatus.php?type=adstart&status=start&sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8&cmpro=3797&cmpid=12610&pid=2047&cid=IN&vid=764258&pfid=2&sc=2016-04-09%3A23%3A59%3A53&ptm=2&mvt=NA&vpu=-1&mvtro=NA&vpuro=0&cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn2 = new UserTransaction(entry);
		entry = "{ \"time\": \"10/Apr/2016:00:00:21 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=first;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn3 = new UserTransaction(entry);
		entry = "{ \"time\": \"10/Apr/2016:00:00:23 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=mid;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn4 = new UserTransaction(entry);
		entry = "{ \"time\": \"10/Apr/2016:00:00:29 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=third;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn5 = new UserTransaction(entry);
		entry = "{ \"time\": \"10/Apr/2016:00:00:35 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=complete;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn6 = new UserTransaction(entry);
		entry = "{ \"time\": \"10/Apr/2016:00:00:35 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=videoengengagement;status=waiting;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;vid=764258;vrid=62023836;pid=2047;cid=IN;cpid=659;ptm=html5 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn7 = new UserTransaction(entry);
		entry = "{ \"time\": \"10/Apr/2016:00:00:37 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=videostart;status=start;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;vid=764258;vrid=62023836;pid=2047;cid=IN;cpid=659 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		UserTransaction txn8 = new UserTransaction(entry);
		
		List<UserTransaction> values = new ArrayList<UserTransaction>();
		values.add(txn1);
		values.add(txn2);
		values.add(txn3);
		values.add(txn4);
		values.add(txn5);
		values.add(txn6);
		values.add(txn7);
		values.add(txn8);

		reduceDriver.withInput(new Text(sessionId), values);
		
		List<Pair<Text, SessionDetails>> res = reduceDriver.run();
		String output = "1.39.15.185--1457966462,44c5f50e-95c1-48a4-98fb-0f4279c249f8,09:23,,1,0,764258,News,killed Murdersuicide Texas base Sheriff,1 0 0 0 0 1 0,3797:12610:1 4 0 0 0 0 0,";
		
		Assert.assertEquals(sessionId, res.get(0).getFirst().toString());
		Assert.assertEquals(output, res.get(0).getSecond().toString());
		
	}

	@Test
	public void testMapReduce() throws IOException {
		
		String sessionId = "44c5f50e-95c1-48a4-98fb-0f4279c249f8";
		
		String entry = "{ \"time\": \"10/Apr/2016:00:00:00 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/userinfo.gif?q=44c5f50e-95c1-48a4-98fb-0f4279c249f8%23%3A%231.39.86.171%23%3A%232047%23%3A%23IN%23%3A%23764258%23%3A%232016-04-09%3A23%3A59%3A53%23%3A%23GJ%23%3A%231.39.15.185--1457966462%23%3A%230%23%3A%2311%23%3A%231%23%3A%23Chrome+Mobile+33.0%23%3A%23Android+4.4+KitKat%23%3A%23450%23%3A%23325%23%3A%23Adsession%3A12610%2Cpreroll%2C0%7C12611%2Cpostroll%2C0%23%3A%23true%23%3A%23false%23%3A%230.1%23%3A%23%23%3A%230%23%3A%23289%23%3A%23Smart+Phone%23%3A%230%23%3A%23 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.withInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:10 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"31\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/istatus.php?type=adstart&status=start&sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8&cmpro=3797&cmpid=12610&pid=2047&cid=IN&vid=764258&pfid=2&sc=2016-04-09%3A23%3A59%3A53&ptm=2&mvt=NA&vpu=-1&mvtro=NA&vpuro=0&cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:21 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=first;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:23 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=mid;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:29 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=third;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:35 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=adprogress;status=complete;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;cmpro=3797;cmpid=12610;pid=2047;cid=IN;vid=764258;pfid=2;sc=2016-04-09%3A23%3A59%3A53;ptm=2;mvt=NA;vpu=-1;mvtro=NA;vpuro=0;cmprate=0 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:35 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=videoengengagement;status=waiting;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;vid=764258;vrid=62023836;pid=2047;cid=IN;cpid=659;ptm=html5 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.addInput(new LongWritable(), new Text(entry));
		entry = "{ \"time\": \"10/Apr/2016:00:00:37 +0530\", \"remote_addr\": \"10.0.1.95\", \"client_ip\": \"1.39.86.171\", \"remote_user\": \"-\", \"body_bytes_sent\": \"43\", \"request_time\": \"0.000\", \"status\": \"200\", \"request\": \"GET /plugins/status.gif?type=videostart;status=start;sid=44c5f50e-95c1-48a4-98fb-0f4279c249f8;vid=764258;vrid=62023836;pid=2047;cid=IN;cpid=659 HTTP/1.1\", \"request_method\": \"GET\", \"http_referrer\": \"http://marathi.eenaduindia.com/News/International/2016/04/09110738/a-mother-wants-to-have-baby-with-her-son.vpf\", \"http_user_agent\": \"Mozilla/5.0 (Linux; Android 4.4.4; SM-G850Y Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/69.0.0.26.76;]\" }";
		mapReduceDriver.addInput(new LongWritable(), new Text(entry));
		
		List<Pair<Text, SessionDetails>> res = mapReduceDriver.run();
		String output = "1.39.15.185--1457966462,44c5f50e-95c1-48a4-98fb-0f4279c249f8,09:23,,1,0,764258,News,killed Murdersuicide Texas base Sheriff,1 0 0 0 0 1 0,3797:12610:1 4 0 0 0 0 0,";
		Assert.assertEquals(sessionId, res.get(0).getFirst().toString());
		Assert.assertEquals(output, res.get(0).getSecond().toString());
		
	}
	
}
