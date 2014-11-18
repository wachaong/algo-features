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
//import com.autohome.adrd.algo.click_model.source.autohome.LabelInstanceSingleOld.HReduce;




public class TableJoin extends AbstractProcessor{
	
	public static class ChannelpvMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String[] lines = value.toString().split("\t");

			StringBuilder sb = new StringBuilder();
			int startnum=1;
			for (int i=startnum;i<lines.length;i++){
				if(!lines[i].equals("0")&&!lines[i].trim().equals("")){
					//String[] feature=lines[i].split(",");
					if(i > startnum)
					{
						sb.append("\t");
					}
					sb.append(lines[i]);
					//for(int j=0;j<feature.length ;j++){
					//	if(!feature[j].equals(""))
					//		sb.append(feature[j]+"\t");
				    //}	
				}
			}
		
			
			//if(sb.toString().length()!=0){
			context.write(new Text(lines[0]), new Text(sb.toString()));

			}

		}
		
	
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String label = new String("");
			String feature = new String("");
			int feature_num = 0;
			
			StringBuilder labelsb= new StringBuilder ();
			StringBuilder featuresb= new StringBuilder ();

			for (Text value : values) {
				
				String[] lines=value.toString().split("\t");
			
				for(int i=0;i<lines.length;i++){
					if(lines[i].equals("label:0")){
						label="0";

					}
					else if(lines[i].equals("label:1")){
						label="1";

					}
					else{
						if(feature_num > 0)
							featuresb.append("\t");
						feature_num++;
						featuresb.append(lines[i]);
					}
				}
			}
			
			
			//add new cookie bias
			if(!label.equals("")){
				if(featuresb.toString().length()!=0){
					context.write(new Text(label), new Text(featuresb.toString()));
				}//else{
				//	context.write(new Text(label), new Text("new:1"));
				//}
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