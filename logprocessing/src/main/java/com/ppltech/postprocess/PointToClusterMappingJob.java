package com.ppltech.postprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PointToClusterMappingJob extends Configured {

  private Path clusteredPointsPath;
  private Path clusterOutPath;

  public PointToClusterMappingJob(Path clusteredPointsPath, Path clusterOutPath) {
    this.clusteredPointsPath = clusteredPointsPath;
    this.clusterOutPath = clusterOutPath;
  }

  public void run() throws IOException, ClassNotFoundException, InterruptedException {
    Configuration configuration = getConf();

    Job job = new Job(configuration, PointToClusterMappingJob.class.getSimpleName());
    job.setInputFormatClass(SequenceFileInputFormat.class);
//    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setJarByClass(PointToClusterMappingJob.class);

    SequenceFileInputFormat.addInputPath(job, clusteredPointsPath);
    SequenceFileOutputFormat.setOutputPath(job, clusterOutPath);

    job.setMapperClass(PointToClusterMapper.class);
    job.setReducerClass(PointToClusterReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.waitForCompletion(true);
  }
}
