package com.autohome.adrd.algo.click_model.source.autohome;

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
import com.autohome.adrd.algo.click_model.data.SparseVector;
import com.autohome.adrd.algo.click_model.data.writable.SingleInstanceWritable;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.click_model.utility.MyPair;

/**
 * @brief Get all the feature names appearing in a sample set.
 * @author Yang Mingmin
 *
 */
public class SparkLRPredict extends AbstractProcessor {
	public static class SparkLRPredictMapper extends Mapper<LongWritable, Text, Text, Text> {
		private SparseVector weight_map = new SparseVector();
		private double weight_square = 0;
		public void setup(Context context) throws FileNotFoundException {
			String map_file = context.getConfiguration().get("weight");
			Scanner in = new Scanner(new File(map_file));
			while(in.hasNext())
				weight_map.setValue(in.nextInt(), in.nextDouble());
			weight_square = weight_map.square();
		}
		
		private SingleInstanceWritable sparkLR2Sample(String str) {
			String tmp = str.trim();
			SingleInstanceWritable s = new SingleInstanceWritable();
			String[] tokens = tmp.split("\t");
			s.setLabel(Double.valueOf(tokens[0]));
			for(int i = 2; i < tokens.length; ++i) {
				String[] kv_pair = tokens[i].trim().split(":");
				s.addIdFea(Integer.valueOf(kv_pair[0]));

			}
			return s;

		}
		
		private double predict(SingleInstanceWritable instance, SparseVector w) {
			double ans = 0.0;
			for(Integer id : instance.getId_fea_vec()) {
				ans += w.getValue(id);
			}
			ans += w.getValue(0);  //adding bias
			if(ans > 35)
				return 1.0;
			else if(ans < -35)
				return 0.0;
			else 
				return 1.0 / (1 + Math.exp(-ans));
		}
		
		public void map(LongWritable k,  Text s, Context context) 
				throws IOException, InterruptedException {
			SingleInstanceWritable instance = sparkLR2Sample(s.toString());
			double prob = predict(instance, weight_map);
			String label = instance.getLabel() > 0.5 ? "1" : "0";
			context.write(new Text(label), new Text("1\t" + prob));
		}
	}
	
	@Override
	protected void configJob(Job job) {
		job.setMapperClass(SparkLRPredictMapper.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
	}
}
