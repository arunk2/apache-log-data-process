package com.ppltech.preprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserExtracterJob extends Configured {

  public static final String INPUT = UserExtracterJob.class.getSimpleName() + "-input";
  public static final String OUTPUT = UserExtracterJob.class.getSimpleName() + "-output";
  public static final String OUTPUT_USERS = "users/";

  public UserExtracterJob(Configuration configuration) {
    super(configuration);
  }

  public Path run() throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = getConf();

    Job job = new Job(configuration);
    job.setInputFormatClass(SequenceFileInputFormat.class);
//    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    Path inputPath = new Path(configuration.get(INPUT));
    Path outputBasePath = new Path(configuration.get(OUTPUT));
    Path textOutputPath = new Path(outputBasePath, OUTPUT_USERS);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, textOutputPath);

    job.setJarByClass(UserExtractorMapper.class);
    job.setMapperClass(UserExtractorMapper.class);
    job.setReducerClass(UserExtractorReducer.class);
//    job.setNumReduceTasks(1);
//    job.setNumReduceTasks(0);

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("User extraction failed processing");
    }

    return textOutputPath;
  }
}
