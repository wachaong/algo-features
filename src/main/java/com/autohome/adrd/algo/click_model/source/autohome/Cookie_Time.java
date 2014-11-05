package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.autohome.adrd.algo.protobuf.ApplogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation.AutoPVInfo;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;
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
		
		private static String pred_date;
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,behavior,tags,addisplay,adclick,pv");
			pred_date = context.getConfiguration().get("pred_date"); 		
		}

		@SuppressWarnings({ "unchecked", "deprecation" })
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
					Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_date.replaceAll("/", ""));
					long diff = d2.getTime() - d.getTime();
					Long days = diff/(1000*60*60*24);
					context.write(new Text(cookie), new Text("cookietime:" + days.toString()));
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