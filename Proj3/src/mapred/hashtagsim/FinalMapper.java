package mapred.hashtagsim;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	// We just output the output of mapper.
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] buffer = line.split("\\s+", 2);
		context.write(new Text(buffer[0]), new IntWritable(Integer.valueOf(buffer[1])));			
	}
}

