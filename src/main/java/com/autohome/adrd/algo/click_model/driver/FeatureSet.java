package com.autohome.adrd.algo.click_model.driver;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
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
public class FeatureSet extends AbstractProcessor {
	public static class FeatureSetMapper extends Mapper<NullWritable, Sample, Text, NullWritable> {
		public void map(NullWritable k,  Sample s, Context context) 
				throws IOException, InterruptedException {
			for(String fea : s.getIdFeatures()) {
				context.write(new Text(fea), NullWritable.get());
			}
			for(Map.Entry<String, Double> entry : s.getFloatFeatures().entrySet()) {
				context.write(new Text(entry.getKey()), NullWritable.get());
			}
		}
	}
	
	public static class FeatureSetReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			for(@SuppressWarnings("unused") NullWritable value : values) {
				context.write(key, NullWritable.get());
				return;
			}
		}
	}
	
	@Override
	protected void configJob(Job job) {
		job.setMapperClass(FeatureSetMapper.class);
		job.setCombinerClass(FeatureSetReducer.class);
		job.setReducerClass(FeatureSetReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);	
	}
}
