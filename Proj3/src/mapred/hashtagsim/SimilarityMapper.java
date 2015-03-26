package mapred.hashtagsim;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {

	Map<String, Integer> jobFeatures = null;

	/**
	 * We compute the inner product of feature vector of every hashtag
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] hashtag_featureVector = line.split("\\s+", 2);
		context.write(new Text(hashtag_featureVector[0]),new Text(hashtag_featureVector[1]));

	}

}