package com.autohome.adrd.algo.click_model.train_preprocess;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

public class LineCount extends AbstractProcessor {
	
	public static class HMap extends Mapper<NullWritable,Sample,  Text, LongWritable> {
		private static float sample_freq;
		private static long sample_freq_inverse;
		
		public void setup(Context context) {
			sample_freq = context.getConfiguration().getFloat("sample_freq", 1.0f);
			sample_freq_inverse = Math.round(1.0/sample_freq);
		}
		
		public void map( NullWritable key, Sample value, Context context) 
				throws IOException, InterruptedException {
			
			if(value.getLabel() >0)
				context.write(new Text("total"), new LongWritable(1));
			else
				context.write(new Text("total"), new LongWritable(sample_freq_inverse));
			
			for(String fea : value.getIdFeatures())
			{
				if(value.getLabel() > 0)
					context.write(new Text(fea), new LongWritable(1));
				else
					context.write(new Text(fea), new LongWritable(sample_freq_inverse));
			}
			
			for(Entry<String, Double> fea : value.getFloatFeatures().entrySet())
			{
				if(value.getLabel() > 0)
					context.write(new Text(fea.getKey()), new LongWritable(1));
				else
					context.write(new Text(fea.getKey()), new LongWritable(sample_freq_inverse));
			}
			
		}
		
	}

	public static class HCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) 
		throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}	
	
	public static class HReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		private static long threshold;
		
		public void setup(Context context) {
			threshold = context.getConfiguration().getLong("threshold", 100);
		}
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			if (sum > threshold)
				context.write(key, new LongWritable(sum));
		}
	}
	
	@Override
	protected void configJob(Job job) {
		// TODO Auto-generated method stub
		job.setMapperClass(HMap.class);
		job.setReducerClass(HReduce.class);
		job.setCombinerClass(HCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(LongWritable.class);
	}
}