package com.ppltech.postprocess;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

/**
 * Maps the name of a point to the id of the cluster, sorted by point name in ascending order.
 */
public class PointToClusterMapper extends Mapper<IntWritable, WeightedVectorWritable, IntWritable, Text> {

  private Text pointName = new Text();

  @Override
  protected void map(IntWritable clusterId, WeightedVectorWritable point, Context context) throws IOException, InterruptedException {
    NamedVector namedVector;
    if (point.getVector() instanceof NamedVector) {
      namedVector = (NamedVector) point.getVector();
    } else {
      throw new RuntimeException("Cannot output point name, point is not a NamedVector");
    }

    pointName.set(namedVector.getName());
    System.out.println("cluster = "+ clusterId.toString() + " --------------: "+ point.getVector());

    context.write(clusterId, pointName);
    
  }
}
