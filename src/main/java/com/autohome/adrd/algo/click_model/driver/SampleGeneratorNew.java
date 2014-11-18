package com.autohome.adrd.algo.click_model.driver;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SampleGeneratorNew extends AbstractProcessor {
	
	public static class SampleGeneratorNewMapper extends Mapper<LongWritable, Text, NullWritable, Sample> {
		private SampleGeneratorHelper helper = new SampleGeneratorHelper();
		
		protected void setup(Context context) {
			String config_file = context.getConfiguration().get("config_file");
	        helper.setup(config_file);
		}

		public void map(LongWritable k1, Text v1, Context context) 
				throws IOException, InterruptedException {
			Sample s = helper.process(v1);
			if(s != null) {
				context.write(NullWritable.get(), s);
			}		
		}
	}
	
	@Override
	protected void configJob(Job job) {
		job.setMapperClass(SampleGeneratorNewMapper.class);
	    job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sample.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Sample.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
	}
}