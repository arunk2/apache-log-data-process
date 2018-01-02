package com.ppltech.main;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import com.ppltech.cluster.VectorizeJob;
import com.ppltech.postprocess.PointToClusterMappingJob;

public class UserTxnCluster {

    private Configuration configuration = new Configuration();
    private String outputPath = "target/output-base/clusters";
    private String inputPath = "target/output-base/users";

    public static void main(String[] args) throws Exception {
    	
    	System.setOut(new PrintStream(new FileOutputStream("output.txt")));
    	
        UserTxnCluster runner = new UserTxnCluster();
        
        if(args.length > 1) {
        	runner.inputPath = args[0];
        	runner.outputPath = args[1];
    	}
        runner.run();
    }

    private void run() throws Exception {
    	
    	cleanOutputBasePath();
    	preProcess();
//        vectorize();
//        cluster();
//        postProcess();
        
    }
    
    private void cleanOutputBasePath() throws IOException {
        HadoopUtil.delete(configuration, new Path(outputPath));
    }
    
    private void preProcess() throws Exception {

    	configuration.set(VectorizeJob.INPUT, inputPath);
        configuration.set(VectorizeJob.OUTPUT, outputPath);

        VectorizeJob job = new VectorizeJob(configuration);
        job.run();
		
	}

	private void vectorize() throws Exception {
		
//        String[] seq2SparseArgs = new String[]{
//                "--input", outputPath+"/keywords",
//                "--output", outputPath+"/sparse",
//                "--maxNGramSize", "1",
//                "--namedVector",
//                "--maxDFPercent", "25",
//                "--norm", "2",
//                "--analyzerName", "com.ventuno.util.KeywordsAnalyzer",
//                "--overwrite"
//        };
//        
//        ToolRunner.run(configuration, new SparseVectorsFromSequenceFiles(), seq2SparseArgs);
		
		calculateTfIdf();
		
    }
	
	private void calculateTfIdf() throws ClassNotFoundException, IOException,
			InterruptedException {
		
		Path documentsSequencePath = new Path(outputPath, "keywords");
        Path tokenizedDocumentsPath = new Path(outputPath,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
        Path tfidfPath = new Path(outputPath, "tfidf");
        Path termFrequencyVectorsPath = new Path(outputPath
                , DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
		
        DocumentProcessor.tokenizeDocuments(documentsSequencePath,
        		com.ppltech.util.KeywordsAnalyzer.class, tokenizedDocumentsPath, configuration);

        DictionaryVectorizer.createTermFrequencyVectors(tokenizedDocumentsPath,
                new Path(outputPath),
                DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER,
                configuration, 1, 1, 0.0f, PartialVectorMerger.NO_NORMALIZING,
                true, 1, 100, false, false);

        Pair<Long[], List<Path>> documentFrequencies = TFIDFConverter
                .calculateDF(termFrequencyVectorsPath, tfidfPath,
                        configuration, 100);

        TFIDFConverter.processTfIdf(termFrequencyVectorsPath, tfidfPath,
                configuration, documentFrequencies, 1, 100,
                PartialVectorMerger.NO_NORMALIZING, false, false, false, 1);
        
	}
    
    private void cluster() throws Exception {
    	
//        String[] kmeansDriver = {
//                "--input", outputPath+"/sparse/tfidf-vectors",
//                "--output", outputPath+"/kmeans",
//                "--clusters", outputPath+"/kmeans-initial-clusters",
//                "--maxIter", "10",
//                "--numClusters", "10",
//                "--convergenceDelta", "0.1",
//                "--distanceMeasure", CosineDistanceMeasure.class.getName(),
//                "--clustering",
//                "--method", "sequential",
//                "--overwrite"
//                
//        };
//
//        ToolRunner.run(configuration, new KMeansDriver(), kmeansDriver);
    	
    	clusterDocs();
        
        dumpClusters();
        
    }
    
	private void clusterDocs() throws ClassNotFoundException, IOException,
			InterruptedException {
		String vectorsFolder = outputPath + "/tfidf/tfidf-vectors";
		String canopyCentroids = outputPath + "/canopy-centroids";
		String clusterOutput = outputPath + "/clusters";

//		FileSystem fs = FileSystem.get(configuration);
//		Path oldClusterPath = new Path(clusterOutput);
//
//		if (fs.exists(oldClusterPath)) {
//			fs.delete(oldClusterPath, true);
//		}
//
//		CanopyDriver.run(new Path(vectorsFolder), new Path(canopyCentroids),
//				new EuclideanDistanceMeasure(), 20, 5, true, true);

		FuzzyKMeansDriver.run(configuration, new Path(vectorsFolder), new Path(canopyCentroids, "clusters-0-final"), new Path(clusterOutput),
				new EuclideanDistanceMeasure(), 0.01, 1, 2, true, true, 0, false);
		
	}
    
	private void dumpClusters() throws Exception {

		String[] terms = new String[100000];
		Path path = new Path(outputPath + "/sparse/dictionary.file-0");
		SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(configuration), path, configuration);
		WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
		Writable value = (Writable) reader.getValueClass().newInstance();

		int i = 0;
		while (reader.next(key, value)) {
			terms[i++] = key.toString();
		}
		reader.close();

		// run ClusterDumper
		ClusterDumper clusterDumper = new ClusterDumper(
				finalClusterPath(configuration, new Path(outputPath + "/kmeans"), 10), 
				new Path(outputPath + "/kmeans", "clusteredPoints"));
		clusterDumper.printClusters(terms);

	}

	private void postProcess() throws Exception {
    	
    	Path outputBasePath = new Path(outputPath);
        Path outputClusteringPath = new Path(outputBasePath, "kmeans");
        Path clusteredPointsPath = new Path(outputClusteringPath, "clusteredPoints");
        Path clusterOutPath = new Path(outputBasePath, "clusterOutPath");

        PointToClusterMappingJob pointsToClusterMappingJob = new PointToClusterMappingJob(clusteredPointsPath, clusterOutPath);
        pointsToClusterMappingJob.setConf(configuration);
        pointsToClusterMappingJob.run();
        
    }
    
    /**
     * Return the path to the final iteration's clusters
     */
    private static Path finalClusterPath(Configuration conf, Path output, int maxIterations) throws IOException {
      FileSystem fs = FileSystem.get(conf);
      for (int i = maxIterations; i >= 0; i--) {
        Path clusters = new Path(output, "clusters-" + i);
        if (fs.exists(clusters)) {
          return clusters;
        }
      }
      return null;
    }
}
