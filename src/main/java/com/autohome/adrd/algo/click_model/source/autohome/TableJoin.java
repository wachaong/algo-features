package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;


public class TableJoin extends AbstractProcessor{
	
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] lines = value.toString().split("\t");
			StringBuilder trainfeature = new StringBuilder();
			StringBuilder testfeature = new StringBuilder();
			int startnum=1;
				for (int i=startnum;i<lines.length;i++){
					if(!lines[i].equals("0")&&!lines[i].equals("")){
						if (lines[i].contains("tr_")) 
							trainfeature.append(lines[i]+"\t");
						else if (lines[i].contains("te_"))
							testfeature.append(lines[i]+"\t");		
						}
					}
				
				
				if(trainfeature.toString().length()!=0)
					context.write(new Text(lines[0]),new Text(trainfeature.toString()));
				if(testfeature.toString().length()!=0)
					context.write(new Text(lines[0]),new Text(testfeature.toString()));
				

		

			}
			

		}
		
		
	
	
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text,Text> multipath;
		String trainpath;
		String testpath;
		public void setup(Context context) throws IOException,InterruptedException {
			multipath = new MultipleOutputs(context);
			trainpath = context.getConfiguration().get("trainpath");
			testpath = context.getConfiguration().get("testpath");
			
			
	    }
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder trainfeature = new StringBuilder();
			StringBuilder testfeature = new  StringBuilder();
			int trainpv = 0;
			int trainsale = 0;
			int testpv = 0;
			int testsale = 0;
			
			
			for (Text value : values) {
				
				String[] lines=value.toString().split("\t");
			
				for(int i=0;i<lines.length;i++){
					if(lines[i].equals("tr_label:0")){
						trainpv = 1;
					}else if(lines[i].equals("tr_label:1")){
						trainsale = 1;
					}else if(lines[i].equals("te_label:0")){
						testpv = 1;
					}else if(lines[i].equals("te_label:1")){
						testsale = 1;
					}else{ 
						if (lines[i].contains("tr_")) 
							trainfeature.append(lines[i].substring(3)+"\t");
						else if (lines[i].contains("te_"))
							testfeature.append(lines[i].substring(3)+"\t");
						
					}
				}
				
				
			}
			
			//过滤掉没行为的cookie
			
			if(trainsale==1){
				if(trainfeature.toString().length()!=0)
					multipath.write(new Text("1"), new Text(trainfeature.toString()),trainpath);
			}
			else if(trainpv == 1){
				if(trainfeature.toString().length()!=0)
					multipath.write(new Text("0"), new Text(trainfeature.toString()),trainpath);
			}
			
			
			if(testsale == 1){
				if(testfeature.toString().length()!=0)
					multipath.write(new Text("1"), new Text(testfeature.toString()),testpath);			
			}
			else if(testpv == 1){
				if(testfeature.toString().length()!=0)
					multipath.write(new Text("0"), new Text(testfeature.toString()),testpath);	
			}
				
			

			
		
			
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException {
			multipath.close();
		}
		
		
	}
	
	protected void configJob(Job job) {

	    job.setMapperClass(JoinMapper.class);
		job.setReducerClass(HReduce.class);
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
	}
}