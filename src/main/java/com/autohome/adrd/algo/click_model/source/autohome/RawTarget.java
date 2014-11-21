package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.PvlogOperation;

/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 * 输出用户过去一段时间内常浏览的车型，车系, 车型价格
 * 以及计算用户喜欢车型 车系 车型价格的集中程度:类似异众比率的计算方式
 * 异众比率:specRatio1,3 seriesRatio1,3
 * 感兴趣的车型车系个数:seriesCnt specCnt
 * 感兴趣的车型价格均值和方差:specVar specMean
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
		private static int days_history;
				

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,behavior,tags,addisplay,adclick,pv");
			pred_date = context.getConfiguration().get("pred_date");
			decay = context.getConfiguration().getDouble("decay",0.6);
			days_history = context.getConfiguration().getInt("history_days", 7);					
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

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			decode(key, value);

			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			String cookie = (String) list.get("user");
			
			String path=((FileSplit)context.getInputSplit()).getPath().toString();
			String date = path.split("sessionlog")[1].split("part")[0].replaceAll("/", "");
			Date d;
			
			try {
				d = new SimpleDateFormat("yyyyMMdd").parse(date);
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_date.replaceAll("/", ""));
				long diff = d2.getTime() - d.getTime();
				long days = diff/(1000*60*60*24);
				
				if(pvList != null && pvList.size() > 0)
				{
					HashMap<String, Integer> dc = new HashMap<String, Integer>();
					for(PvlogOperation.AutoPVInfo pvinfo : pvList) {
						try {
							int series = Integer.parseInt(pvinfo.getSeriesid());
							add("seriesId" + days_history + "@" + series, dc);
							
							int spec = Integer.parseInt(pvinfo.getSpecid());
							add("specId"+ days_history + "@" + spec, dc);
						}
						catch(Exception e) {
							;
						}						
					}
					
					if(cookie != null  && !cookie.isEmpty() && !dc.isEmpty())
						context.write(new Text(cookie), new Text(output_map (dc, days)));	
					
				}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	}

	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		private static int days_history;
		private Map<String,String> spec_price_map = new HashMap<String,String>();
				
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			days_history = context.getConfiguration().getInt("history_days", 7);
			//spec_price_map = CommonDataAndFunc.readMaps("click_cookie", CommonDataAndFunc.TAB, 0, 1, "utf-8");
			String spec_price_map_file = context.getConfiguration().get("spec_price");
			Scanner in = new Scanner(new File(spec_price_map_file));
			while(in.hasNext()) {
				spec_price_map.put(in.next(), in.next());
			}
			
			//spec_price_map = CommonDataAndFunc.readMaps(spec_price_map_file, CommonDataAndFunc.TAB, 0, 1, "utf-8");
		}
		
		private void string2dict(String str, HashMap<String, Double> spec, HashMap<String, Double> series) {
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
				if(key.contains("spec"))
				{
					if(spec.containsKey(key)) {
						spec.put(key, spec.get(key) + val);
					}
					else {
						spec.put(key, val);
					}
				}
				else if(key.contains("series"))
				{
					if(series.containsKey(key)) {
						series.put(key, series.get(key) + val);
					}
					else {
						series.put(key, val);
					}
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

				sb.append(val);				
			}
			return sb.toString();
		}


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Double> spec_score = new HashMap<String, Double>();
			HashMap<String, Double> series_score = new HashMap<String, Double>();
			for (Text value : values) {
				if(value.toString().trim().isEmpty())
					continue;
				string2dict(value.toString(), spec_score, series_score);
			}
			
			HashMap<String, Double> spec_score_tmp = new HashMap<String, Double>();
			HashMap<String, Double> series_score_tmp = new HashMap<String, Double>();
			for(Map.Entry<String, Double> entry : spec_score.entrySet()) {
				String ID = entry.getKey().trim().split("@")[1];
				spec_score_tmp.put(ID, entry.getValue());
			}
			for(Map.Entry<String, Double> entry : series_score.entrySet()) {
				String ID = entry.getKey().trim().split("@")[1];
				series_score_tmp.put(ID, entry.getValue());
			}
			
			/*
			 * 用户是否决定购买某款车型 还是在选择多款车型的阶段
			 *  
			 * */
			List<Map.Entry<String, Double>> series_lst = new ArrayList<Map.Entry<String, Double>>(series_score_tmp.entrySet());
			List<Map.Entry<String, Double>> spec_lst = new ArrayList<Map.Entry<String, Double>>(spec_score_tmp.entrySet());
			
			Collections.sort(series_lst, new Comparator<Map.Entry<String, Double>>() {   
			    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {      
			        return (int) (o2.getValue() - o1.getValue());			        
			    }
			});
			
			Collections.sort(spec_lst, new Comparator<Map.Entry<String, Double>>() {   
			    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {      
			        return (int) (o2.getValue() - o1.getValue());			        
			    }
			});

			double ratio_top1 = 0.0, ratio_top3 = 0.0, sum = 0.0;			
			int spec_cnt = 0, series_cnt = 0;
			
			for (int i = 0; i < series_lst.size(); i++) {
			    if(i == 0)
			    	ratio_top1 += series_lst.get(i).getValue();
			    if(i<3)
			    	ratio_top3 += series_lst.get(i).getValue();
			    sum += series_lst.get(i).getValue();
			    if(series_lst.get(i).getValue() > 2)
			    	series_cnt++;			    
			}
			if(sum > 0) {
				ratio_top1 = ratio_top1 / sum;
				ratio_top3 = ratio_top3 / sum;				
			}

			series_score.put("seriesRatio1"+ days_history , ratio_top1);
			series_score.put("seriesRatio3"+ days_history , ratio_top3);
			series_score.put("seriesCnt"+ days_history , (double) series_cnt);
			
			ratio_top1 = 0.0; ratio_top3 = 0.0; sum = 0.0;
			double price_mean = 0.0, price_var = 0.0;
			int cnt_var = 0, sum_var = 0;
			
			for (int i = 0; i < spec_lst.size(); i++) {
			    if(i == 0)
			    	ratio_top1 += spec_lst.get(i).getValue();
			    if(i<3)
			    {
			    	ratio_top3 += spec_lst.get(i).getValue();
			    	if(spec_price_map.containsKey(spec_lst.get(i).getKey())) {
			    		price_mean += Double.valueOf(spec_price_map.get(spec_lst.get(i).getKey()));
			    		cnt_var ++;
			    	}
			    	
			    	
			    }
			    sum += spec_lst.get(i).getValue();
			    if(spec_lst.get(i).getValue() > 2)
			    	spec_cnt++;		
			}
			if(cnt_var > 0)
				price_mean /= cnt_var;
			for (int i = 0; i < spec_lst.size(); i++) {
				if(i>=3)
					break;
				if(spec_price_map.containsKey(spec_lst.get(i).getKey()))
					sum_var += Math.pow(Double.valueOf(spec_price_map.get(spec_lst.get(i).getKey())) - price_mean, 2);				
			}
			if(cnt_var > 0)
			{
				price_var = Math.sqrt(sum_var/cnt_var);
			}
			
			if(sum > 0)
			{
				ratio_top1 = ratio_top1 / sum;
				ratio_top3 = ratio_top3 / sum;
			}
			spec_score.put("specRatio1"+ days_history , ratio_top1);
			spec_score.put("specRatio3"+ days_history , ratio_top3);
			series_score.put("specCnt"+ days_history , (double) spec_cnt);
			series_score.put("specVar"+ days_history , price_var);
			series_score.put("specMean"+ days_history , price_mean);
			
			spec_score.putAll(series_score);
			
			if(!spec_score.isEmpty())			
			    context.write(key, new Text(output_map(spec_score)));
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