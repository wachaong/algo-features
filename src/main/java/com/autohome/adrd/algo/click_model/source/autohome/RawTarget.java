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
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.click_model.data.SparseVector;
import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.autohome.adrd.algo.protobuf.ApplogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation.AutoPVInfo;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation.SaleleadsInfo;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;

/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 */

public class RawTarget extends AbstractProcessor {
	
	

	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";
		public static final String CG_SEARCH = "search";
		public static final String CG_SALE_LEADS = "saleleads";
		public static final String CG_TAGS = "tags";
		public static final String CG_APPPV = "apppv";
		public static final String CG_BEHAVIOR = "behavior";
		
		private static String pred_date;
		private static double decay;
		

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,behavior,tags,addisplay,adclick,pv");
			pred_date = context.getConfiguration().get("pred_date");
			decay = context.getConfiguration().getDouble("decay",0.6);
			////equal value discretization
			
		}
		
		private void add(String fea, HashMap<String, Integer> map) {
			if(map.containsKey(fea)) {
				map.put(fea, map.get(fea) + 1);
			}
			else
				map.put(fea, 1);	
		}
		

		private String output_map(HashMap<String, Integer> map, long days) {
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for(Map.Entry<String, Integer> entry : map.entrySet()) {
				if(i > 0)
					sb.append("\t");
				i++;
				sb.append(entry.getKey());
			    sb.append("\t");
				int val = entry.getValue();
				if(val > 50)
					val = 50;
				sb.append(val * Math.pow(decay,days));
									
			}
			return sb.toString();
		}

		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			
			List<SaleleadsInfoOperation.SaleleadsInfo> saleleadsList = new ArrayList<SaleleadsInfoOperation.SaleleadsInfo>();
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			
			Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
			decode(key, value);

			saleleadsList = (List<SaleleadsInfoOperation.SaleleadsInfo>) list.get(CG_SALE_LEADS);
			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			String cookie = (String) list.get("user");
			
			String path=((FileSplit)context.getInputSplit()).getPath().toString();
			String date = path.split("sessionlog")[1].split("part")[0].replaceAll("/", "");
			Date d;
			
				try {
					d = new SimpleDateFormat("yyyyMMdd").parse(date);
			
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_date);
				long diff = d2.getTime() - d.getTime();
				long days = diff/(1000*60*60*24);
				
				//int saleleads_cnt = 0, pv_cnt = 0;
				pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
				
				
				if(pvList != null && pvList.size() > 0)
				{
					HashMap<String, Integer> dc = new HashMap<String, Integer>();
					HashMap<String, Integer> dc_spec = new HashMap<String, Integer>();
					for(PvlogOperation.AutoPVInfo pvinfo : pvList) {
						String seriesId = pvinfo.getSeriesid();
						int series;
						try {
							series = Integer.parseInt(seriesId);
							add("seriesId@" + series, dc);
						}
						catch(Exception e) {
							;
						}
						
						String specId = pvinfo.getSeriesid();
						int spec;
						try {
							spec = Integer.parseInt(specId);
							add("specId@" + spec, dc);
						}
						catch(Exception e) {
							;
						}

					}
					
					if(cookie != null  && !cookie.isEmpty() && !dc.isEmpty())
						context.write(new Text(cookie), new Text(output_map(dc, days)));	
					
				}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
	}
	}

	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		private static double discret;
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			////equal value discretization
			discret = context.getConfiguration().getDouble("discret",0.01);
		}
		
		private void string2dict(String str, HashMap<String, Double> ans) {
			if(str == null)
				return;
			String key = null;
			double val;
			String[] tmp = str.trim().split("\t");
			if(tmp.length == 0 || tmp.length % 2 != 0)
				return;
			for(int i = 0; i < tmp.length / 2; i += 2)
			{
				key = tmp[i];
				val = Double.parseDouble(tmp[i+1]);
				if(ans.containsKey(key)) {
					ans.put(key, ans.get(key) + val);
				}
				else {
					ans.put(key, val);
				}
			}
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
				//equal value discretization
				int discretval=(int)(val/discret);
				sb.append(discretval);
				
			}
			return sb.toString();
		}


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Double> dc_score = new HashMap<String, Double>();
			for (Text value : values) {
				if(value.toString().trim().isEmpty())
					continue;
				string2dict(value.toString(), dc_score);
			}
			if(!dc_score.isEmpty())
				
			    context.write(key, new Text(output_map(dc_score)));
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