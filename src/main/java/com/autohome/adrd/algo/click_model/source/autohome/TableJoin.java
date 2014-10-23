package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.StatusReporter;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.click_model.source.autohome.LabelInstanceSingleOld.HReduce;




public class TableJoin extends AbstractProcessor{
	
	public static class ChannelpvMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String[] lines = value.toString().split("\t");
			StringBuilder sb = new StringBuilder();
			int startnum=0;
			if(lines.length==9){
				startnum=2;
				
				if(!lines[1].equals("0")){
				    String[] day =lines[1].split(" ");
					if(day.length==2){
					String[] score=day[1].split(":");
					if(day.length==2&&score.length==3)
						sb.append(day[0]+":"+score[2]+"\t");
					
				}
			}else if(lines.length==2){
				startnum=1;
			}else{
				//add label
				context.write(new Text(lines[0]),new Text("label"+"\t"+lines[1]));
				startnum=2;
			}
			
			
			for (int i=startnum;i<lines.length;i++){
				if(!lines[i].equals("0"))
					sb.append(lines[i]+"\t");
			}
			
			if(sb.toString().length()!=0){
				context.write(new Text(lines[0]),new Text(sb.toString()));
			}
			
		}
		
		
	}
	}
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String label = new String("");
			String feature = new String("");
			
			StringBuilder labelsb= new StringBuilder ();
			StringBuilder featuresb= new StringBuilder ();
			for (Text value : values) {
				if(value.toString().split("\t").length==9){
					
					label = value.toString();
					labelsb.append(value.toString());
				}
				else if(value.toString().split("\t").length==2){
					feature=value.toString();
					featuresb.append(value.toString());
					
				}
			}
			
			
			if((!feature.equals(""))&&(!label.equals(""))){
				context.write(new Text(label), new Text(feature));
			}
			
			
			
		}
	}
	
	protected void configJob(Job job) {

		job.setMapperClass(ChannelpvMapper.class);
		job.setReducerClass(HReduce.class);
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
	}
}