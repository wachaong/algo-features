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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;

/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 * version1
 * 输出用户过去一段时间内常浏览的车型，车系, 车型价格
 * 以及计算用户喜欢车型 车系 车型价格的集中程度:类似异众比率的计算方式
 * 异众比率:specRatio1,3 seriesRatio1,3
 * 感兴趣的车型车系个数:seriesCnt specCnt
 * 感兴趣的车型价格均值和方差:specVar specMean
 * 
 * version2
 * 生成用户特征集合，主要覆盖的是时序相关的基于pv日志的behavior targeting特征
 * 支持pred日期之前的多个时间段的特征的同时输出，支持同时输出训练集和测试集特征,或者只是生成训练集特征
 * 包括:频道兴趣，车型，车系，价格，用户属性等
 * 
 */

public class RawTarget extends AbstractProcessor {
		
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";		
		public static final String CG_APPPV = "apppv";
		public static final String CG_SALE_LEADS = "saleleads";
		
		private static String pred_train_start;
		private static String pred_test_start;
		private static String days_history;
				

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,apppv,pv");
			pred_train_start = context.getConfiguration().get("pred_train_start");
			//if pred_test_start set to no, then don't generate test set
			pred_test_start = context.getConfiguration().get("pred_test_start");	
			days_history = context.getConfiguration().get("history_days", "7:0.8,15:0.9,30:0.95,60:0.975");					
		}
		
		private void add(String fea, HashMap<String, Double> map, double score) {
			if(map.containsKey(fea)) {
				map.put(fea, map.get(fea) + score);
			}
			else
				map.put(fea, score);	
		}
		

		private String output_map(HashMap<String, Double> map) {
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for(Map.Entry<String, Double> entry : map.entrySet()) {
				if(i > 0)
					sb.append("\t");
				i++;
				sb.append(entry.getKey());
			    sb.append("\t");
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
			List<SaleleadsInfoOperation.SaleleadsInfo> saleleadsList = new ArrayList<SaleleadsInfoOperation.SaleleadsInfo>();
			
			decode(key, value);

			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			saleleadsList = (List<SaleleadsInfoOperation.SaleleadsInfo>) list.get(CG_SALE_LEADS);
			
			String cookie = (String) list.get("user");
			
			String path=((FileSplit)context.getInputSplit()).getPath().toString();
			String date = path.split("sessionlog")[1].split("part")[0].replaceAll("/", "");
			Date d;
			Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
			
			try {
				
				d = new SimpleDateFormat("yyyyMMdd").parse(date);
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_train_start.replaceAll("/", ""));
				long diff = d2.getTime() - d.getTime();
				long days_train = diff/(1000*60*60*24);  //ѵ����˥�����
				long days_test = 999;
				int series=0;
				int spec=0;
				if(! pred_test_start.equals("no"))
				{
					d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_test_start.replaceAll("/", ""));
					diff = d2.getTime() - d.getTime();
					days_test = diff/(1000*60*60*24);  //ѵ����˥�����
				}
				
				if(pvList != null && pvList.size() > 0)
				{
					HashMap<String, Double> dc = new HashMap<String, Double>();
					for(PvlogOperation.AutoPVInfo pvinfo : pvList) {
						
						if(pattern.matcher(pvinfo.getSeriesid()).matches())
							series = Integer.valueOf(pvinfo.getSeriesid());	
						if(pattern.matcher(pvinfo.getSpecid()).matches())
							spec = Integer.valueOf(pvinfo.getSpecid());
							
					
						
						String province = pvinfo.getProvinceid().trim();
						String city = pvinfo.getCityid().trim();
						add("province" + "@" + province, dc, 1.0);
						add("city" + "@" + city, dc, 1.0);
						
						for(String part : days_history.split(","))
						{
							int day = Integer.valueOf(part.split(":",2)[0]);
							double decay = Double.valueOf(part.split(":",2)[1]);
							if( (days_train > 0) && (days_train <= day) )
							{
								double score = Math.pow(decay,days_train);
								if(pattern.matcher(pvinfo.getSeriesid()).matches())
									add("tr_series_" + String.valueOf(day) + "@" + series, dc, score);
								if(pattern.matcher(pvinfo.getSpecid()).matches())
									add("tr_spec_"+ String.valueOf(day) + "@" + spec, dc, score);																								
								
								if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
									add("tr_channel_" + String.valueOf(day) + "@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id(), dc, score);
							}
							if( days_test <= day )
							{
								double score = Math.pow(decay,days_test);
								add("te_series_" + String.valueOf(day) + "@" + series, dc, score);
								add("te_spec_"+ String.valueOf(day) + "@" + spec, dc, score);
								
								if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
									add("te_channel_" + String.valueOf(day) + "@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id(), dc, score);
							}																											
						}																							
					}
					
					if (saleleadsList != null && saleleadsList.size() != 0)
					{
						for(String part : days_history.split(","))
						{
							int day = Integer.valueOf(part.split(":",2)[0]);
							double decay = Double.valueOf(part.split(":",2)[1]);
							if( (days_train > 0) && (days_train <= day) )
							{
								double score = Math.pow(decay,days_train);
								add("tr_hissaleleads_" + String.valueOf(day), dc, score);								
							}
							if( days_test <= day )
							{
								double score = Math.pow(decay,days_test);
								add("te_hissaleleads_" + String.valueOf(day), dc, score);								
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

	/*
	 * 为map阶段的所有特征增加如下特征：
	 */
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		private Map<String,Double> spec_price_map = new HashMap<String,Double>();
				
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			String spec_price_map_file = context.getConfiguration().get("spec_price");
			Scanner in = new Scanner(new File(spec_price_map_file));
			while(in.hasNext()) {
				spec_price_map.put(in.next(), Double.valueOf(in.next()));
			}
		}
		
		private void string2dict(String str, HashMap<String, Double> dic, HashSet<String> types) {
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
				if(key.indexOf("@") != -1)
				{
					types.add(key.split("@")[0]);
				}
				if(dic.containsKey(key)) {
					dic.put(key, dic.get(key) + val);
				}
				else {
					dic.put(key, val);
				}
			}
		}
		
		private List<Entry<String, Double>> filter_from_dict(String filter, HashMap<String, Double> dic) {
			HashMap<String, Double> subset = new HashMap<String, Double>();
			for(Map.Entry<String, Double> entry : dic.entrySet()) {
				if(entry.getKey().trim().indexOf(filter) != -1)
				{
					subset.put(entry.getKey(), entry.getValue());
				}
			 }
			
			List<Map.Entry<String, Double>> dic_lst = new ArrayList<Map.Entry<String, Double>>(subset.entrySet());
			Collections.sort(dic_lst, new Comparator<Map.Entry<String, Double>>() {   
			    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {      
			        return (int) (o2.getValue() - o1.getValue());			        
			    }
			});
			
			return dic_lst;			
		}
		
		private HashMap<String, Double> ratio_features(String prefix, List<Map.Entry<String, Double>> sort_lst, 
				Map<String,Double> spec_price_map) {
			HashMap<String, Double> new_feas = new HashMap<String, Double>();
			
			double ratio_top1 = 0.0, ratio_top3 = 0.0, sum = 0.0, price_mean = 0.0, price_var = 0.0;
			int cnt = 0, cnt_var = 0, sum_var = 0;
			boolean do_price = false;
			if( sort_lst.get(0).getKey().indexOf("spec") != -1 )
				do_price = true;
			
			for (int i = 0; i < sort_lst.size(); i++) {
			    if(i == 0)
			    	ratio_top1 += sort_lst.get(i).getValue();
			    if(i<3)
			    {
			    	ratio_top3 += sort_lst.get(i).getValue();
			    	if(do_price)
			    	{
				    	if(spec_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1])) {
				    		price_mean += Double.valueOf(spec_price_map.get(sort_lst.get(i).getKey().split("@")[1]));
				    		cnt_var ++;
				    	}
			    	}
			    		
			    }
			    sum += sort_lst.get(i).getValue();
			    if(sort_lst.get(i).getValue() > 2)
			    	cnt++;			    
			}
			if(sum > 0) {
				ratio_top1 = ratio_top1 / sum;
				ratio_top3 = ratio_top3 / sum;				
			}

			new_feas.put(prefix + "_Ratio1" , ratio_top1);
			new_feas.put(prefix + "_Ratio3" , ratio_top3);
			new_feas.put(prefix + "_Cnt" , (double) cnt);
			
			if(do_price)
			{
				if(cnt_var > 0)
					price_mean /= cnt_var;
				for (int i = 0; i < sort_lst.size(); i++) {
					if(i>=3)
						break;
					if(spec_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1]))
						sum_var += Math.pow(Double.valueOf(spec_price_map.get(sort_lst.get(i).getKey().split("@")[1])) - price_mean, 2);				
				}
				if(cnt_var > 0)
				{
					price_var = Math.sqrt(sum_var/cnt_var);
					new_feas.put(prefix + "_Mean" , price_mean);
					new_feas.put(prefix + "_Var" , price_var);
				}
			}
										
			return new_feas;
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
			HashMap<String, Double> dic = new HashMap<String, Double>();
			HashMap<String, Double> dic_tmp, dic_add;
			List<Map.Entry<String, Double>> sort_lst;
			HashSet<String> types = new HashSet<String>();
			
			for (Text value : values) {
				if(value.toString().trim().isEmpty())
					continue;
				string2dict(value.toString(), dic, types);
			}
			
			/*
			 * 用户是否决定购买某款车型 还是在选择多款车型的阶段
			 *  
			 * */
			dic_tmp = new HashMap<String, Double>();
			for(String type : types)
			{
				sort_lst = filter_from_dict(type, dic);
				dic_add = ratio_features(type, sort_lst, spec_price_map);
				dic_tmp.putAll(dic_add);
				dic_add.clear();
			}
						
			dic.putAll(dic_tmp);
			
			if(!dic.isEmpty())			
			    context.write(key, new Text(output_map(dic)));
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