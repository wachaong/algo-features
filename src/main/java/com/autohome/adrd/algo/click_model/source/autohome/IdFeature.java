package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;


public class IdFeature extends AbstractProcessor{
	
	public static class PredictMapper extends Mapper<LongWritable, Text, Text, Text>{

		 private static String id_name;
		 static  Map<String, String>  featureid;
		 

		
		protected void setup(Context context) throws IOException,InterruptedException {

			id_name = context.getConfiguration().get("id_file", "featureid");
			featureid =  CommonDataAndFunc.readMapBySep(id_name, "utf8", 2);
			

		}
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
			String[] lines = value.toString().split("\t");
			StringBuilder sb = new StringBuilder();

			if(lines.length>1){

				for(int i=1;i<lines.length;i++){
							
					String[] featurescore = lines[i].split(":");
					if(featurescore.length==2){
						if(featureid.containsKey(featurescore[0])){
							sb.append(featureid.get(featurescore[0])+":"+featurescore[1]+"\t");
						}
						
						
					
							
					}
						
						    
				}
				
					
				}
				context.write(new Text(lines[0]),new Text(sb.toString()));
		  }
				
			}
		
		
	
	protected void configJob(Job job) {

		job.setMapperClass(PredictMapper.class);
	
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

	}
}
