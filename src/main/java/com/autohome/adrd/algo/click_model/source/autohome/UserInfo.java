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
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation.SaleleadsInfo;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;


/**
 * 
 * �û���ʡ�ݺͳ���
 *
 */
public class UserInfo extends AbstractProcessor {
	
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";
		public static final String CG_SEARCH = "search";
		public static final String CG_SALE_LEADS = "saleleads";
		public static final String CG_TAGS = "tags";
		public static final String CG_APPPV = "apppv";
		public static final String CG_BEHAVIOR = "behavior";

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,behavior,tags,addisplay,adclick,pv");
		}
		
		private String dc_max(HashMap<String, Integer> dc) {
			int max_val = -1;
			String key = "";
			for(Map.Entry<String, Integer> ent : dc.entrySet())
			{
				if(max_val < ent.getValue())
				{
					key = ent.getKey();
					max_val = ent.getValue();
				}
			}
			return key;
		}

		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			
			decode(key, value);
			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			String cookie = (String) list.get("user");
			
			if(pvList != null && pvList.size() > 0)
			{
				HashMap<String, Integer> dc_province = new HashMap<String, Integer>();
				HashMap<String, Integer> dc_city = new HashMap<String, Integer>();
				for(PvlogOperation.AutoPVInfo pvinfo : pvList){
					String province = pvinfo.getProvinceid();
					String city = pvinfo.getCityid();
					province = province.trim();
					city = city.trim();
					if(!province.isEmpty())
					{
						if(dc_province.containsKey(province))
							dc_province.put(province, 1 + dc_province.get(province));
						else
							dc_province.put(province, 1);
					}
					if(!city.isEmpty())
					{
						if(dc_city.containsKey(province))
							dc_city.put(city, 1 + dc_city.get(city));
						else
							dc_city.put(city, 1);
					}
				}
				
				if(!dc_province.isEmpty())
					context.write(new Text(cookie), new Text("province@" +
				                                             dc_max(dc_province) + ":1" + "\t" +
							                                 "city@" + dc_max(dc_city) + ":1"));

			}
													
		}
	}

	public static class HReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String , Double> map = new HashMap<String , Double>(); 
			for (Text value : values) {
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
