package com.ppltech.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import com.ppltech.postprocess.PointToClusterMappingJob;
import com.ppltech.preprocess.LogParserJob;
import com.ppltech.preprocess.UserExtracterJob;

/**
 * Main class for running log extraction jobs, and clustering users
 * with the video content and other details extracted.
 * 
 * @author dev
 *
 */
public class Main {

    private Configuration configuration = new Configuration();
    private Path outputBasePath = new Path("target/output-base/");

    private Path outputSeq2SparsePath;
    private Path outputVectorPath;

    public static void main(String[] args) throws Exception {
        Main runner = new Main();
        runner.run();
    }

    private void run() throws Exception {
        cleanOutputBasePath(); 
        preProcess();
        vectorize();
        cluster();
        postProcess();
    }

    private void cleanOutputBasePath() throws IOException {
        HadoopUtil.delete(configuration, outputBasePath);
    }

    private void preProcess() throws ClassNotFoundException, IOException, InterruptedException {
    	
        outputSeq2SparsePath = new Path(outputBasePath, "sparse");
        outputVectorPath = new Path(outputSeq2SparsePath, "tfidf-vectors");

        configuration.set(LogParserJob.INPUT, "/home/dev/Work/LogFile/12-Apr-2016/log/nginx");
        configuration.set(LogParserJob.OUTPUT, outputBasePath.toString());

        LogParserJob parseJob = new LogParserJob(configuration);
        parseJob.run();
        
        configuration.set(UserExtracterJob.INPUT, new Path(outputBasePath, LogParserJob.OUTPUT_SESSION_PATH).toString());
        configuration.set(UserExtracterJob.OUTPUT, outputBasePath.toString());

        UserExtracterJob extracterJob = new UserExtracterJob(configuration);
        extracterJob.run();
        
    }

    private void vectorize() throws Exception {
        String[] seq2SparseArgs = new String[]{
                "--input", new Path(outputBasePath, UserExtracterJob.OUTPUT_USERS).toString(),
                "--output", outputSeq2SparsePath.toString(),
                "--maxNGramSize", "1",
                "--namedVector",
                "--maxDFPercent", "25",
                "--norm", "2",
                "--analyzerName", "com.ventuno.util.KeywordsAnalyzer",
                "--overwrite"
        };

        ToolRunner.run(configuration, new SparseVectorsFromSequenceFiles(), seq2SparseArgs);
    	
    }
    
    private void cluster() throws Exception {
        String algorithmSuffix = "kmeans";

        Path outputKMeansPath = new Path(outputBasePath, algorithmSuffix);

        String[] kmeansDriver = {
                "--input", outputVectorPath.toString(),
                "--output", outputKMeansPath.toString(),
                "--clusters", "target/kmeans-initial-clusters",
                "--maxIter", "10",
                "--numClusters", "10",
                "--distanceMeasure", CosineDistanceMeasure.class.getName(),
                "--clustering",
                "--method", "sequential",
                "--overwrite"
        };

        ToolRunner.run(configuration, new KMeansDriver(), kmeansDriver);
    }
    
    private void postProcess() throws Exception {
    	
        Path outputClusteringPath = new Path(outputBasePath, "kmeans");
        Path clusteredPointsPath = new Path(outputClusteringPath, "clusteredPoints");
        Path pointsToClusterPath = new Path(outputBasePath, "pointsToClusters");

        PointToClusterMappingJob pointsToClusterMappingJob = new PointToClusterMappingJob(clusteredPointsPath, pointsToClusterPath);
        pointsToClusterMappingJob.setConf(configuration);
        pointsToClusterMappingJob.run();
        
    }

}
