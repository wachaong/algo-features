package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
/**
 * 
 * @author [Chen shuaihua ]
 * 
 *对输入的sample的特征highlow二值化，输出 FM sample
 * 
 */

public class ReachCTRDiscFeature extends AbstractProcessor{
	
	public static class FeatureMapper extends Mapper<LongWritable, Text, Text, Text>{
		private static String dicrete;
		private static String idthresholdfile;
		static  Map<String, String>  idfeature = new HashMap<String,String>();
		private static String id_name;
		static  Map<String, String>  idthreshold = new HashMap<String,String>();
		private static String id_highlow;
		static Map<String,String> highlow_featureid =new HashMap<String,String>();
		private String filename = null;
		public static Map<String, String> readMapBySep(String fileName,
				String encoding, int sepPos) {
			Map<String, String> map = new HashMap<String, String>();
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(fileName), encoding));
				String line;
				while ((line = reader.readLine()) != null) {
					if (line.startsWith("#") || line.trim().isEmpty()) {
						continue;
					}
					String[] tokens = line.split("\t", sepPos);
					map.put(tokens[0], tokens[1]);
				}
				reader.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return map;
		}
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			//default discrete 0.1
			dicrete = context.getConfiguration().get("dicrete","0.1"); 
			filename = context.getConfiguration().get("idthreshold");
			idthresholdfile = context.getConfiguration().get("filename","idthreshold");
			idthreshold =  readMapBySep(idthresholdfile, "utf8", 2);
			filename = context.getConfiguration().get("idfeature");
			id_name = context.getConfiguration().get("filename","idfeature");
			idfeature =  readMapBySep(id_name, "utf8", 2);
			filename = context.getConfiguration().get("highlow_featureid");
			id_highlow = context.getConfiguration().get("filename","highlow_featureid");
			highlow_featureid=readMapBySep(id_highlow, "utf8", 2);
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	

			
			
			String[] lines = value.toString().split("\t");

			StringBuilder sb = new StringBuilder();
			int startnum=1;
			for (int i=startnum;i<lines.length;i++){
				if(!lines[i].equals("0")&&!lines[i].trim().equals("") ){
					String[] idscore=lines[i].split(":");
					if(idthreshold.containsKey(idscore[0])) {
				
							String id;
							
					    	int threshold = (int)(Float.parseFloat(idthreshold.get(idscore[0])));
					    	int score=(int)(Float.parseFloat(idscore[1])/Double.parseDouble(dicrete));
					    	if(score> threshold){
					    		if(highlow_featureid.containsKey(idfeature.get(idscore[0])+"_high")){
					    			 id = highlow_featureid.get(idfeature.get(idscore[0])+"_high");
					    			 sb.append((Integer.parseInt(id))+":1"+"\t");
				
					    			 
					    		}
					    		else if(highlow_featureid.containsKey(idfeature.get(idscore[0]))){
					    			id = highlow_featureid.get(idfeature.get(idscore[0]));
					    			sb.append((Integer.parseInt(id))+":1"+"\t");
				
					    		}
					    	}
					    	else{
					    		id = highlow_featureid.get(idfeature.get(idscore[0])+"_low");
					    		sb.append((Integer.parseInt(id))+":1"+"\t");
					    	
					    	}
					    	
					}

				}
			}
		    if(!sb.toString().isEmpty())
		    	context.write(new Text(sb.toString()),new Text(lines[0]));
		    
		    
		    
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
			

			context.write( new Text(String.valueOf(clk_cnt) + "\t" + String.valueOf(pv_cnt)),key);
		}
	}
	
	
	
	
	
	
	
	protected void configJob(Job job) {

		job.setMapperClass(FeatureMapper.class);
		job.setReducerClass(HReduce.class);
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
	}
}
