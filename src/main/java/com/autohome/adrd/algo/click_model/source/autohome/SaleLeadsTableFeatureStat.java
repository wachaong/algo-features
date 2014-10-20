package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;


public class SaleLeadsTableFeatureStat extends AbstractProcessor{
	
	public static class StatMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//	String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString(); 
			//InputSplit inputSplit = context.getInputSplit();
		//	String fileName = ((FileSplit) inputSplit).getPath().toString();
			String[] lines = value.toString().split("\t");
			StringBuilder sb = new StringBuilder();
			if(lines.length>2){
				for(int i=1;i<lines.length;i++)
					if(i==2){
						context.write(new Text("Timeid"+"\t"+i+"\t"+(int)Float.parseFloat(lines[i])),new Text(lines[0]));
					}
					else{
						context.write(new Text("Timeid"+"\t"+i+"\t"+Integer.parseInt(lines[i])),new Text(lines[0]));
					}
			}
	
		}
		
		
	}
	
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int pv_cnt = 0, clk_cnt = 0;
			for (Text value : values) {
				if (value.toString().equals("1")) {
					pv_cnt += 1;
					clk_cnt += 1;
				}
				if (value.toString().equals("0")) {
					pv_cnt += 1;
				}
			}

			context.write( key,new Text(String.valueOf(clk_cnt) + "\t" + String.valueOf(pv_cnt)));
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
