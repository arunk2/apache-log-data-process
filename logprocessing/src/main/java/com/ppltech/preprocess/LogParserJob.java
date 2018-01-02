package com.ppltech.preprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.ppltech.prepross.extract.SessionDetails;

public class LogParserJob extends Configured {

  public static final String INPUT = LogParserJob.class.getSimpleName() + "-input";
  public static final String OUTPUT = LogParserJob.class.getSimpleName() + "-output";

  public static final String OUTPUT_SESSION_PATH = "sessions/";

  private Path postOutputPath;

  public LogParserJob(Configuration configuration) {
    super(configuration);
  }

  public Path run() throws ClassNotFoundException, IOException, InterruptedException {
    Configuration configuration = getConf();

    Job job = new Job(configuration);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(UserTransaction.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SessionDetails.class);

    Path inputPath = new Path(configuration.get(INPUT));
    Path outputBasePath = new Path(configuration.get(OUTPUT));
    postOutputPath = new Path(outputBasePath, OUTPUT_SESSION_PATH);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, postOutputPath);

    job.setJarByClass(LogParserJob.class);
    job.setMapperClass(LogParserMapper.class);
    job.setReducerClass(LogParserReducer.class);
//    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("Log parsing failed processing");
    }

    return postOutputPath;
  }

  public Path getPostOutputPath() {
    return postOutputPath;
  }
}
