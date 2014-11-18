package com.autohome.adrd.algo.click_model.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

/**
 * @brief Get all the feature names appearing in a sample set.
 * @author Yang Mingmin
 *
 */
public class Sample2SparkLR extends AbstractProcessor {
	public static class Sample2SparkLRMapper extends Mapper<NullWritable, Sample, Text, NullWritable> {
		private HashMap<String, Long> feature_id_map = new HashMap<String, Long>();
		public void setup(Context context) throws FileNotFoundException {
			String map_file = context.getConfiguration().get("feature_id_file");
			Scanner in = new Scanner(new File(map_file));
			while(in.hasNext())
				feature_id_map.put(in.next(), in.nextLong());
			
		}
		public void map(NullWritable k,  Sample s, Context context) 
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			sb.append((int)s.getLabel());
			sb.append("\t1");
			boolean has_feature = false;
			for(String fea : s.getIdFeatures()) {
				if(feature_id_map.containsKey(fea)) {
					sb.append("\t" + feature_id_map.get(fea) + ":1");
					has_feature = true;
				}
			}
			for(Map.Entry<String, Double> entry : s.getFloatFeatures().entrySet()) {
				String fea = entry.getKey();
				if(feature_id_map.containsKey(fea)) {
					sb.append("\t" + feature_id_map.get(fea) + ":" + entry.getValue());
					has_feature = true;
				}
			}
			if(has_feature)
				context.write(new Text(sb.toString()), NullWritable.get());
		}
	}
	
	@Override
	protected void configJob(Job job) {
		job.setMapperClass(Sample2SparkLRMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);	
	}
}
