package com.autohome.adrd.algo.click_model.source.kaggle;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.zebra.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.io.*;

import org.apache.hadoop.io.Text;

public class KaggleCount /*extends AbstractProcessor*/ {
	
	public static class KaggleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			KaggleSource ks = new KaggleSource();
			DoubleWritable result = new DoubleWritable();
			if(key.get() > 1) {
				Sample s = ks.process(value);
				double label = s.getLabel();
				result.set(label);
				for(String fea : s.getIdFeatures()) {
					
					context.write(new Text(fea), result);
				}
				
				for(Map.Entry<String, Double> entry : s.getFloatFeatures().entrySet()) {
					String fea = entry.getKey();
					double val = entry.getValue();
					Integer v = new Integer((int)val);
					context.write(new Text(fea + "@" + v.toString()), result);
				}
			}
		}

	}
	
	public static class IntSumReducer 
      extends Reducer<Text,DoubleWritable,Text,Text> {
	//private Text result =new Text();
   /**
        * Reducer类中的reduce方法�?
     * void reduce(Text key, Iterable<IntWritable> values, Context context)
        * 中k/v来自于map函数中的context,可能经过了进�?��处理(combiner),同样通过context输出           
        */
   public void reduce(Text key, Iterable<DoubleWritable> values, 
                      Context context
                      ) throws IOException, InterruptedException {
     double sum =0;
     int cnt = 0;
     for (DoubleWritable val : values) {
       sum += val.get();
       cnt++;
     }
     //result.set(sum);
     
     String tmp = "" + cnt + "," + ((int)sum);
     //result.set(tmp);
     context.write(key, new Text(tmp));
   }
 }
	
	protected void configJob(Job job) {


		//SumCombiner<Text> s = new SumCombiner<Text>();
		job.setMapperClass(KaggleMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

	}

	/*public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String value = Long.toString(4 * 67108864L);
        conf.set("mapred.min.split.size", value);
        conf.set("table.input.split.minSize", value);
		Job job = new Job(conf, "Kaggle Count");
		job.setJarByClass(KaggleCount.class);
		job.setMapperClass(KaggleMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(50);
		FileInputFormat.addInputPath(job, new Path(args[0])) ;
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}*/
	
}
