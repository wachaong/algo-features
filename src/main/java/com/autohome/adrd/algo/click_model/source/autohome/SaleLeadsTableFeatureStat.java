package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

/**
 * Generate feature CLK PV
 * in order to Draw Reach-CTR
 * @author : Chen Shuaihua
 */

public class SaleLeadsTableFeatureStat extends AbstractProcessor{
	
	public static class StatMapper extends Mapper<LongWritable, Text, Text, Text>{
		private static String dicrete;
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//default discrete 0.1
			dicrete = context.getConfiguration().get("dicrete","0.1"); 		
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
			String[] lines = value.toString().split("\t");
			StringBuilder sb = new StringBuilder();
			if(lines.length>1){
				for(int i=1;i<lines.length;i++){
					String[] featurescore = lines[i].split(":");
					if(featurescore.length==2){
						//featurescore equal value discrete
					    context.write(new Text(featurescore[0]+"\t"+(int)(Float.parseFloat(featurescore[1])/Double.parseDouble(dicrete))),new Text( lines[0]  ));
					   
					}
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
