package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation.AutoPVInfo;
import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;

public class UserFeature extends AbstractProcessor {

	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";	

		private static String pred_start;
		private static String days_history;

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,pv");
			pred_start = context.getConfiguration().get("pred_start");
			days_history = context.getConfiguration().get("history_days", "7:0.8,15:0.9,30:0.95");

		}
		
		private void add(String fea, HashMap<String, Double> map, double score) {
			if(map.containsKey(fea)) {
				map.put(fea, map.get(fea) + score);
			}
			else
				map.put(fea, score);	
		}
		
		private void addFeature(String fea, HashMap<String, Double> map, double score){
			if(fea != null && fea.length() > 0)
				add(fea, map, score);
		}
		
		private String formatTime(String inTime,long unit,long u){
			String rst = null;
			try {
				   Date d = new SimpleDateFormat("yyyyMMddHHmmss").parse(inTime.replaceAll("-", "").replaceAll(":", ""));
				   rst = String.valueOf((d.getTime()/unit)%u);
				 }catch (ParseException e) {
					 //TODO Auto-generated catch block
			}
			return rst;
		}
		private String output_map(HashMap<String, Double> map) {
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for(Map.Entry<String, Double> entry : map.entrySet()) {
				if(i > 0)
					sb.append("\t");
				i++;
				sb.append(entry.getKey());
			    sb.append(":");
				double val = entry.getValue();
				if(val > 50)
					val = 50;
				sb.append(val);									
			}
			return sb.toString();
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			
			decode(key, value);

			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);	
		
			String cookie = (String) list.get("user");
			if(cookie.length()>36)
				cookie=cookie.substring(0,35);
			String path=((FileSplit)context.getInputSplit()).getPath().toString();
			String date = path.split("pv_compress")[1].replaceAll("/", "");
			Date d;
			Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
			
			try {
				
				d = new SimpleDateFormat("yyyyMMdd").parse(date);
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_start.replaceAll("/", ""));
				long diff = d2.getTime() - d.getTime();
				long days = diff/(1000*60*60*24);

				String series="";
				String spec="";
				String cookieTime = null;
				String clickTime = null;

				
				if(pvList != null && pvList.size() > 0)
				{
					HashMap<String, Double> dc = new HashMap<String, Double>();
					for(AutoPVInfo pvinfo : pvList) {
		
						if((!pattern.matcher(pvinfo.getSeriesid()).matches()) || (pvinfo.getSeriesid().equals("")))
							continue;
						if((!pattern.matcher(pvinfo.getSpecid()).matches()) || (pvinfo.getSpecid().equals("")))
							continue;
							
						series = pvinfo.getSeriesid();	
						spec = pvinfo.getSpecid();
						cookieTime = formatTime(pvinfo.getCityname(),1000*60*60*24,365);
						clickTime = formatTime(pvinfo.getVisittime(),1000*60*60,24);
						
						String province = pvinfo.getProvinceid().trim();
						String city = pvinfo.getCityid().trim();

						String[] daysplit = days_history.split(",");
						double maxday = Double.valueOf(daysplit[daysplit.length-1].split(":",2)[0]);
						if( (days > 0) && (days <= maxday) ){
							dc.put("province" + "@" + province, 1.0);
							dc.put("city" + "@" + city, 1.0);
						}
						   
						
						for(String part : days_history.split(","))
						{
							String brand = null;
							String level = null;
	
							//String series_price = null;
				
							int day = Integer.valueOf(part.split(":",2)[0]);
							double decay = Double.valueOf(part.split(":",2)[1]);
							if( (days > 0) && (days <= day) )
							{
								double score = Math.pow(decay,days);
								if(!pvinfo.getSeriesid().equals("0"))
								{
									addFeature("series_" + String.valueOf(day) + "@" + series, dc, score);
									addFeature("clickTime_" + String.valueOf(day) + "@" + clickTime, dc, score);
									addFeature("cookieTime_" + String.valueOf(day) + "@" + cookieTime, dc, score);
									addFeature("brand_" + String.valueOf(day) + "@" + brand, dc, score);
									addFeature("level_" + String.valueOf(day) + "@" + level, dc, score);

								
								}
								
								if(! pvinfo.getSpecid().equals("0"))
									add("spec_"+ String.valueOf(day) + "@" + spec, dc, score);																								
								if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
									add("channel_" + String.valueOf(day) + "@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id(), dc, score);
								
								
								  
							}
																											
						}																							
					}

					
					if(cookie != null  && !cookie.isEmpty() && !dc.isEmpty())
						context.write(new Text(cookie), new Text(output_map (dc)));	
					
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	}

	  

	@Override
	protected void configJob(Job job) {
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(RCFileMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
}
