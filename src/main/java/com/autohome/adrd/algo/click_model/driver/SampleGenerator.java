package com.autohome.adrd.algo.click_model.driver;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

//import org.apache.hadoop.zebra.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.data.writable.SingleInstanceWritable;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;

import java.util.*;
import java.io.FileNotFoundException;
import java.io.IOException;


public class SampleGenerator extends AbstractProcessor{

	
	public static class SampleGeneratorMapper extends Mapper<NullWritable, Sample, SingleInstanceWritable, NullWritable> {
		private SampleGeneratorHelper helper = new SampleGeneratorHelper();
		private Map<String, Integer> feature_id_map = new HashMap<String, Integer>();


		protected void setup(Context context) throws FileNotFoundException {
			String conf_file = context.getConfiguration().get("config_file");
			helper.setup(conf_file);
			String feature_map_file = context.getConfiguration().get("feature_map");
			feature_id_map = helper.readMaps(feature_map_file);
		}

		public void map(NullWritable k1, Sample s, Context context) throws IOException, InterruptedException {
			//source :
			//if(k1.get() > 1) {
				
				SingleInstanceWritable instance = new SingleInstanceWritable();
				instance.setLabel(s.getLabel());

				for(String fea : s.getIdFeatures()) {
					if(feature_id_map.containsKey(fea))
						instance.addIdFea(feature_id_map.get(fea));
				}

				for(Map.Entry<String, Double> ent : s.getFloatFeatures().entrySet()) {
					if(feature_id_map.containsKey(ent.getKey()))
						instance.addFloatFea(feature_id_map.get(ent.getKey()), ent.getValue());
				}

				if(s != null) {
					context.write(instance, NullWritable.get());
				}	
			//}
		}
	}


	@Override
	protected void configJob(Job job) {
		job.setNumReduceTasks(0);
		job.setMapperClass(SampleGeneratorMapper.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(SingleInstanceWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(SingleInstanceWritable.class);
		job.setOutputValueClass(NullWritable.class);
	}


}
