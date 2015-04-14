package hw6;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

public class ApproxPageRank {
	public static void main(String[] args) throws FileNotFoundException{
		if(args.length != 4){
			System.out.println("The number of arguments is wrong!");
		}
		String inputpath = args[0];
		String seed = args[1];
		double alpha = Double.valueOf(args[2]);
		double epsilon = Double.valueOf(args[3]);
		HashMap<String, Double> p = new HashMap<String, Double>();
		HashMap<String, Double> r = new HashMap<String, Double>();
		p.put(seed, 0.0);
		r.put(seed, 1.0);
		boolean notend = true;
		while (notend) {
			notend = false;
			String line = null;
			try {
				BufferedReader br = new BufferedReader(new FileReader(inputpath));
				while ((line = br.readLine()) != null) {
					String[] buffer = line.split("\t");
					String node = buffer[0];
					if (r.containsKey(node)) {
						double outdegree = buffer.length - 1;
						double ru = r.get(node);
						if (outdegree == 0) {
							continue;
						} else if ((ru / outdegree) <= epsilon) {
							continue;
						}
						while ((ru / outdegree) > epsilon) {
							double addition1 = alpha * ru;
							double addition2 = (1D - alpha) * ru * 0.5;
							if (!p.containsKey(node)) {
								p.put(node, addition1);
							} else {
								p.put(node, p.get(node) + addition1);
							}
							if (!r.containsKey(node)) {
								r.put(node, addition2);
							} else {
								r.put(node, r.get(node) + addition2);
							}
							r.put(node, addition2);
							double du = 1 / (2.0 * outdegree);
							double addition3 = (1D - alpha) * ru * du;
							for (int i = 1; i < buffer.length; i++) {
								if (!r.containsKey(buffer[i])) {
									r.put(buffer[i], addition3);
								} else {
									r.put(buffer[i], r.get(buffer[i]) + addition3);
								}
							}
							ru = r.get(node);
						}
						notend = true;
					}
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
			for (Map.Entry<String, Double> entry : p.entrySet()) {
	            bw.append(entry.getKey() + "\t" + entry.getValue() + "\n");
	        }
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
