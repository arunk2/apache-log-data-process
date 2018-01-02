package com.ppltech.cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class VectorizeJob extends Configured {

  public static final String INPUT = VectorizeJob.class.getSimpleName() + "-input";
  public static final String OUTPUT = VectorizeJob.class.getSimpleName() + "-output";

  public static final String KEYWORDS_PATH = "keywords";

  private Path postOutputPath;

  public VectorizeJob(Configuration configuration) {
    super(configuration);
  }

  public Path run() throws ClassNotFoundException, IOException, InterruptedException {
    Configuration configuration = getConf();

    Job job = new Job(configuration);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    Path inputPath = new Path(configuration.get(INPUT));
    Path outputBasePath = new Path(configuration.get(OUTPUT));
    postOutputPath = new Path(outputBasePath, KEYWORDS_PATH);

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, postOutputPath);

    job.setJarByClass(VectorizeJob.class);
    job.setMapperClass(VectorizeMapper.class);
    job.setNumReduceTasks(0);

    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("Vectorize failed processing");
    }

    return postOutputPath;
  }

  public Path getPostOutputPath() {
    return postOutputPath;
  }
}
