package com.ppltech.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

import com.ppltech.preprocess.LogParserJob;
import com.ppltech.preprocess.UserExtracterJob;

public class ExtractLogData {

    private Configuration configuration = new Configuration();
    private String outputPath = "target/output-base/";
    private String inputPath = "logs1";

    public static void main(String[] args) throws Exception {
    	
        ExtractLogData runner = new ExtractLogData();
        if(args.length > 1) {
        	runner.inputPath = args[0];
        	runner.outputPath = args[1];
    	}
        runner.run();    	

    }

    private void run() throws Exception {
        cleanOutputBasePath(); 
        extractData();
    }

    private void cleanOutputBasePath() throws IOException {
        HadoopUtil.delete(configuration, new Path(outputPath));
    }

    private void extractData() throws ClassNotFoundException, IOException, InterruptedException {

        configuration.set(LogParserJob.INPUT, inputPath);
        configuration.set(LogParserJob.OUTPUT, outputPath);

        LogParserJob parseJob = new LogParserJob(configuration);
        parseJob.run();
        
        configuration.set(UserExtracterJob.INPUT, new Path(outputPath+"/"+LogParserJob.OUTPUT_SESSION_PATH).toString());
        configuration.set(UserExtracterJob.OUTPUT, outputPath);

        UserExtracterJob extracterJob = new UserExtracterJob(configuration);
        extracterJob.run();
        
    }

}
