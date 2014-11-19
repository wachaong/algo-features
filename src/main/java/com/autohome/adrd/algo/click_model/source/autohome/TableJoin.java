package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

/**
 * Join  label and Feature
 * @author : Chen Shuaihua
 */
public class TableJoin extends AbstractProcessor{
	
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String[] lines = value.toString().split("\t");
			

			StringBuilder sb = new StringBuilder();
			int startnum=1;
			
		
			for (int i=startnum;i<lines.length;i++){

				if(!lines[i].equals("0")&&!lines[i].equals("")){
					String[] feature=lines[i].split(",");
					for(int j=0;j<feature.length ;j++){
						if(!feature[j].equals(""))
							sb.append(feature[j]+"\t");
					}
					
						
				}
			}
		
			
			
			

				context.write(new Text(lines[0]),new Text(sb.toString()));

	
			

		}
		
		
	}
	
	public static class JoinReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String label = new String("");
			String feature = new String("");
			
			StringBuilder labelsb= new StringBuilder ();
			StringBuilder featuresb= new StringBuilder ();
			for (Text value : values) {
				
				String[] lines=value.toString().split("\t");
			
				for(int i=0;i<lines.length;i++){
					if(lines[i].equals("label:0")){
						label="0";
					}else if(lines[i].equals("label:1")){
						label="1";
					}else{
						featuresb.append(lines[i]+"\t");
					}
				}
				
				
			}
			
			
			//add new cookie bias
			if(!label.equals("")){
				if(featuresb.toString().length()!=0){
					context.write(new Text(label), new Text(featuresb.toString()));
				}else{
					context.write(new Text(label), new Text("new"));
				}
			}
			
			
		}
	}
	
	protected void configJob(Job job) {

		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReduce.class);
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
	}
}