package mapred.hashtagsim;

import java.io.IOException;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SimilarityReducer extends Reducer<Text, Text, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			for (Text word1 : value) {
				String[] buffer1 = word1.toString().split(":", 2);
				int count = Integer.valueOf(buffer1[1]);
				for (Text word2 : value) {
					String[] buffer2 = word2.toString().split(":", 2);
					int count2 = Integer.valueOf(buffer2[1]);
					if (buffer1[0].compareTo(buffer2[0]) < 0) {
						context.write(new Text(buffer1[0] + buffer2[0]), new IntWritable(count * count2));
					}
				}
			}
		
	}
}
