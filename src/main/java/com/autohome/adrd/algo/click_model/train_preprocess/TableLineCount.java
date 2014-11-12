package com.autohome.adrd.algo.click_model.train_preprocess;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;


public class TableLineCount extends AbstractProcessor{
	
	public static class StatMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
			String[] lines = value.toString().split("\t");
			for(String item:lines){
				String[] featurescore=item.split(":");
				if(featurescore.length==2){
					context.write( new Text(featurescore[0]),new Text("1"));
				}
			}
			
				
			}
		}
	
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int  clk_cnt = 0;
			for (Text value : values) {
				if (value.toString().equals("1")) {
					
					clk_cnt += 1;
				}
				
			}
			

			context.write( key,new Text(String.valueOf(clk_cnt)));
		}
	}
	
	protected void configJob(Job job) {

		job.setMapperClass(StatMapper.class);
		job.setReducerClass(HReduce.class);
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
	}
}
