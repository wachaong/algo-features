package com.autohome.adrd.algo.click_model.driver;

import java.io.IOException;

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
public class IdFeatureCount extends AbstractProcessor {
	public static class IdFeatureCountMapper extends Mapper<NullWritable, Sample, Text, LongWritable> {
		public void map(NullWritable k,  Sample s, Context context) 
				throws IOException, InterruptedException {
			LongWritable ONE = new LongWritable(1);
			for(String fea : s.getIdFeatures()) {
				context.write(new Text(fea), ONE);
			}
		}
	}
	
	public static class IdFeatureCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException {
			long count = 0;
			for(LongWritable value : values) {
				count += value.get();
			}
			context.write(key, new LongWritable(count));
		}
	}
	
	@Override
	protected void configJob(Job job) {
		job.setMapperClass(IdFeatureCountMapper.class);
		job.setCombinerClass(IdFeatureCountReducer.class);
		job.setReducerClass(IdFeatureCountReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);	
	}
}
