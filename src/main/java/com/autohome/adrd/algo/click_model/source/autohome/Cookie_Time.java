package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;

/**
 * 用户的注册时长[以天为单位]
 * @author : Yang Mingmin 
 */

public class Cookie_Time extends AbstractProcessor {

	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";
		public static final String CG_SEARCH = "search";
		public static final String CG_SALE_LEADS = "saleleads";
		public static final String CG_TAGS = "tags";
		public static final String CG_APPPV = "apppv";
		public static final String CG_BEHAVIOR = "behavior";
		
		private static String pred_train_start;
		private static String pred_test_start;
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,behavior,tags,addisplay,adclick,pv");
			pred_train_start = context.getConfiguration().get("pred_train_start");
			//if pred_test_start set to no, then don't generate test set
			pred_test_start = context.getConfiguration().get("pred_test_start");	
		}

		@SuppressWarnings({ })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			decode(key, value);
			String cookie = (String) list.get("user");
			TargetingKVOperation.TargetingInfo targeting = (TargetingKVOperation.TargetingInfo) list.get(CG_TAGS);
			
 		    if (targeting != null) {
				String cookie_time = targeting.getCookieTime();
				if(cookie_time == null || cookie_time.trim() == "0")
					return;
				String date = cookie_time.trim().split(" ")[0].trim().replaceAll("-", "");
				Date d;
				try {
					d = new SimpleDateFormat("yyyyMMdd").parse(date);
					Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_train_start.replaceAll("/", ""));
					long diff = d2.getTime() - d.getTime();
					Long days_tr = diff/(1000*60*60*24);
					context.write(new Text(cookie), new Text());
					
					d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_test_start.replaceAll("/", ""));
					diff = d2.getTime() - d.getTime();
					Long days_te = diff/(1000*60*60*24);
					context.write(new Text(cookie), new Text("tr_cookietime:" + days_tr.toString() + "\t" + "te_cookietime:" + days_te.toString()));
				}catch (ParseException e) {
					e.printStackTrace();
				}	
				
			}														
		}
	}

    public static class HReduce extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values, Context context) throws 
    	    IOException, InterruptedException {
    		
    		for(Text value : values) {
    			context.write(key, value);
    			return;
    		}
    	}
    }
    
	@Override
	protected void configJob(Job job) {
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(RCFileMapper.class);
		job.setReducerClass(HReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}