package com.autohome.adrd.algo.click_model.driver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.data.writable.SingleInstanceWritable;
import com.autohome.adrd.algo.click_model.driver.SampleGenerator.SampleGeneratorMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

public class TestFileGenerator extends AbstractProcessor{

	public static class SampleGeneratorMapper extends 
	Mapper<LongWritable, Text, SingleInstanceWritable, NullWritable> {

		public void map(LongWritable k1, Text v1, Context context) 
				throws IOException, InterruptedException {
			//source :
			String str = v1.toString();
			
			String[] tmp = str.trim().split("\t");
			int num_pos = Integer.valueOf(tmp[0]);
			int num_total = Integer.valueOf(tmp[1]);

			int[] id = new int[tmp.length - 2];
			for(int i = 2; i < tmp.length; ++i) {
				String[] tmp2 = tmp[i].split(":");
				id[i-2] = Integer.valueOf(tmp2[0]);
			}
			
			for(int i = 0; i < num_pos; ++i) {
				SingleInstanceWritable instance = new SingleInstanceWritable();
				instance.setLabel(1.0);
				
				for(int j = 0; j < id.length; ++j)
				{
					instance.addIdFea(id[j]);
				}
				context.write(instance, NullWritable.get());
			}
			
			for(int i = 0; i < (num_total - num_pos); ++i) {
				SingleInstanceWritable instance = new SingleInstanceWritable();
				instance.setLabel(0.0);
				
				for(int j = 0; j < id.length; ++j)
				{
					instance.addIdFea(id[j]);
				}
				context.write(instance, NullWritable.get());
			}
		}
	}



	@Override
	protected void configJob(Job job) {
		job.setNumReduceTasks(0);
		job.setMapperClass(SampleGeneratorMapper.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(SingleInstanceWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(SingleInstanceWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
	}

}