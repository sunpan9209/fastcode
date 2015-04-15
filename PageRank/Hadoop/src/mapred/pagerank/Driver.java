package mapred.pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import mapred.job.Optimizedjob;
//import mapred.util.SimpleParser;

import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// This is almost the same to the last time except we parse the number of n and store it using a static int
public class Driver {
	private static NumberFormat nf = new DecimalFormat("00");

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);
		String input = parser.get("input");
		String output = parser.get("output");
		int n = Integer.valueOf(parser.get("n"));
		runParsing(input, output + "/iter00");

		String lastResultPath = null;
		for (int runs = 0; runs < n; runs++) {
			String inPath = output + "/iter" + nf.format(runs);
			lastResultPath = output + "/iter" + nf.format(runs + 1);
			runCalculation(inPath, lastResultPath);
		}
		runOrdering(lastResultPath, output + "/result");
	}

	private static void runParsing(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"parse XML file");
		job.setClasses(WikiLinksMapper.class, WikiLinksReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.run();
	}

	private static void runCalculation(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"calculate the rank");
		job.setClasses(RankCalculateMapper.class, RankCalculateReduce.class,
				null);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.run();
	}

	private static void runOrdering(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"rank ordering");
		job.setClasses(RankingMapper.class, null, null);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.run();
	}
}
