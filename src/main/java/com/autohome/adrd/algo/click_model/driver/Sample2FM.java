package com.autohome.adrd.algo.click_model.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
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
public class Sample2FM extends AbstractProcessor {
	public static class Sample2SparkLRMapper extends Mapper<NullWritable, Sample, Text, NullWritable> {
		private HashMap<String, Integer> feature_id_map = new HashMap<String, Integer>();
		private HashMap<Integer, String> feature_column_map = new HashMap<Integer, String>();
		//private HashMap<Integer, String> column_feature_map = new HashM
		public void setup(Context context) throws FileNotFoundException {
			String map_file = context.getConfiguration().get("feature_id_file");
			String column_file = context.getConfiguration().get("feature_column_file");
			Scanner in = new Scanner(new File(map_file));
			while(in.hasNext())
				feature_id_map.put(in.next(), in.nextInt());
			in = new Scanner(new File(column_file));
			while(in.hasNext()) {
				String tmp1 = in.next();
				Integer tmp2 = in.nextInt();
				feature_column_map.put(tmp2, tmp1);
			}
				
			
		}
		public void map(NullWritable k,  Sample s, Context context) 
				throws IOException, InterruptedException {
			//Vector<Integer> ans = new Vector<Integer>(feature_column_map.size());
			int[] ans = new int[feature_column_map.size()];// = new Array<Integer>();
			for(Map.Entry<Integer, String> ent : feature_column_map.entrySet()) {
				int clm = ent.getKey();
				String fea = ent.getValue();
				ans[clm] =  feature_id_map.get(fea + "_miss");
			}
			
			StringBuilder sb = new StringBuilder();
			sb.append((int)s.getLabel());
			sb.append("\t");
			boolean has_feature = false;
			for(String fea : s.getIdFeatures()) {
				int clm = -1;
				for(Map.Entry<Integer, String> ent : feature_column_map.entrySet()) {
					if(fea.contains(ent.getValue()))
						clm = ent.getKey();
				}
				
				if(feature_id_map.containsKey(fea)) {
					ans[clm] =  feature_id_map.get(fea);
					has_feature = true;
				}
			}
			
			for(int id : ans) {
				sb.append("\t" + id);
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
