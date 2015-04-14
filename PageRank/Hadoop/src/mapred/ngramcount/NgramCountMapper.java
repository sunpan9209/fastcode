package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);
		int l = words.length;
		for (int i = 0; i < l - Driver.n + 1; i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(words[i]);
			for (int j = 1; j < Driver.n; j++) {
				sb.append(" " + words[i + j]);
			}
			context.write(new Text(sb.toString()), one);
		}
	}
}
