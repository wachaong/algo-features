package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.File;
import java.io.IOException;
//import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Scanner;

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
//import com.autohome.adrd.algo.click_model.source.autohome.LabelInstanceSingleOld.HReduce;




public class Subset extends AbstractProcessor{
	
	public static class SubsetMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private HashSet<String> chosen_features = new HashSet<String>();
		private String filename = null;
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			filename = context.getConfiguration().get("feature_list");
			Scanner in = new Scanner(new File(filename));
			while(in.hasNext()) {
				chosen_features.add(in.next());
			}
			in.close();
			chosen_features.add("new");
		}
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String[] lines = value.toString().split("\t");

			StringBuilder sb = new StringBuilder();
			int startnum=1;
			int j = 0;
			for (int i=startnum;i<lines.length;i++){
				if(!lines[i].equals("0")&&!lines[i].trim().equals("") ){
					String[] feature=lines[i].split(":");
					if(chosen_features.contains(feature[0])
							|| feature[0].contains("province")
							|| feature[0].contains("city")) {
					    if(j > 0) {
						    sb.append("\t");
					    }
					    sb.append(lines[i]);
					    j++;
					}

				}
			}
		    if(!sb.toString().isEmpty())
		    	context.write(new Text(lines[0]), new Text(sb.toString()));

			}

		}
		
	

	
	protected void configJob(Job job) {

		job.setMapperClass(SubsetMapper.class);
		//job.setReducerClass(HReduce.class);
	    job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
	}
}