package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);
		int l = words.length;
		for (int i = 0; i < l - Driver.n + 1; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = i; j < Driver.n; j++) {
				sb.append(words[j]);
				sb.append(" ");
			}
			context.write(new Text(sb.toString().trim()), NullWritable.get());
		}
	}
}
