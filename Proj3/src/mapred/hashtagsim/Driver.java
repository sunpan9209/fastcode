package mapred.hashtagsim;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		getHashtagFeatureVector(input, tmpdir + "/feature_vector");
		getHashtagSimilarities(tmpdir + "/feature_vector", tmpdir + "/index");
		finalresult(tmpdir + "/index", output);

	}


	/**
	 * This is the first MapReduce job, it computes all the feature
	 * vector for all hashtags and output the number. Then we output each vector
	 * word as key and the tag their count as value.
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getHashtagFeatureVector(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for all hashtags");
		job.setClasses(HashtagMapper.class, HashtagReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();
	}

	/**
	 * When we have feature vector for all hashtags, we output the Reducer in the last getHashtagFeatureVector
	 * as Mapper and we use each vector as key and output the time that each two pair of tags appear 
	 * together in the Reducer. For each feature word, we output the tag sequentially. To save time, we use a 
	 * HashMap to accelerate the speed.
	 * 
	 * 
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */

	
	private static void getHashtagSimilarities(
			String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get similarities among all other hashtags");
		job.setClasses(SimilarityMapper.class, SimilarityReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();
	}
	
	
	/**
	 * This is the final MapReduce job, we change the type of mapper and reducer to IntWritable and LongWritable
	 * to accelerate the speed. In the Reducer, we just add the count together.
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void finalresult(
			String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get final result");
		job.setClasses(FinalMapper.class, FinalReducer.class, null);
		job.setMapOutputClasses(Text.class, IntWritable.class);
		job.run();
	}
}
