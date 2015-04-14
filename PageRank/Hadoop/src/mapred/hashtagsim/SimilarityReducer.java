package mapred.hashtagsim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterable<Text> value, Context context) 
			throws IOException, InterruptedException {

		HashMap<String, Integer> map = new HashMap<String, Integer>();
		Iterator<Text> iter = value.iterator();
		while (iter.hasNext()) {
			String[] buffer1 = iter.next().toString().split(":", 2);
			map.put(buffer1[0], Integer.valueOf(buffer1[1]));
		}
		ArrayList<String> tags = new ArrayList<String>(map.keySet());
		String outputstr = new String();
		for (int i = 0; i < tags.size(); i ++) {
			for (int j = i + 1; j < tags.size(); j ++) {
				String tag1 = tags.get(i);
				String tag2 = tags.get(j);
				Integer sum = map.get(tag1) * map.get(tag2);
				if (tag1.compareTo(tag2) > 0) {
					outputstr = tag2 + "," + tag1;
				}		
				else if (tag1.compareTo(tag2) < 0) {
					outputstr = tag1 + "," + tag2;
				}	
				context.write(new Text(outputstr), new IntWritable(sum));
			}
		}
	}
}
