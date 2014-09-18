package com.autohome.adrd.algo.click_model.source.kaggle;


import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.autohome.adrd.algo.click_model.data.writable.SingleInstanceWritable;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

public class KaggleAdapter extends AbstractProcessor {
	
	public static class KaggleMapper extends Mapper<LongWritable, Text, SingleInstanceWritable, NullWritable> {
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			SingleInstanceWritable si = new SingleInstanceWritable();
			Vector<Integer> id_fea_vec = new Vector<Integer>();
			
			String [] segs = value.toString().split("\t", -1);
			
			/*
			if (segs[0].equals("0"))
			{
				if(Math.random()>0.1)
					return;
			}
			*/
			
			si.setLabel(Double.valueOf(segs[0]));
			
			for(int i=2; i< segs.length; i++)
			{
				if(segs[i].indexOf(":")!= -1)
				{
					int id = Integer.parseInt(segs[i].split(":")[0]);
					id_fea_vec.add(id);
				}
			}
			
			si.setId_fea_vec(id_fea_vec);
			
			context.write(si, NullWritable.get());			
		}

	}
		
	protected void configJob(Job job) {
		job.setMapperClass(KaggleMapper.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(SingleInstanceWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(SingleInstanceWritable.class);
		job.setOutputValueClass(NullWritable.class);
		String value = Long.toString(4 * 67108864L);
		job.getConfiguration().set("mapred.min.split.size", value);
		job.getConfiguration().set("table.input.split.minSize", value);
	}	
	
}
