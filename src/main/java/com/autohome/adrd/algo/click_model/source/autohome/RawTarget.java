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
 * 异众比率:specRatio1,3 seriesRatio1,3 levelRatio1,3 brandRatio1,3
 * 感兴趣的车型车系品牌级别个数:seriesCnt specCnt levelCnt brandCnt
 * 感兴趣的车型车系价格均值和方差:specVar specMean seriesVar seriesMean
 * 
 * version2
 * 生成用户特征集合，主要覆盖的是时序相关的基于pv日志的behavior targeting特征
 * 支持pred日期之前的多个时间段的特征的同时输出，支持同时输出训练集和测试集特征,或者只是生成训练集特征
 * 包括:频道兴趣，车型，车系，价格，用户属性、click time cookie time madeinChina PvRatio等
 * 
 */

public class RawTarget extends AbstractProcessor {
	
	private static void add2dict(String str,Map<String,Double> dic) throws FileNotFoundException{
		Scanner in = new Scanner(new File(str));
		while(in.hasNextLine()){
			String[] featurevalue= in.nextLine().split("\t");
			dic.put(featurevalue[0],Double.valueOf(featurevalue[1]));
		}
	}

		
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";		
		public static final String CG_APPPV = "apppv";
		public static final String CG_SALE_LEADS = "saleleads";
		
		private static String pred_train_start;
		private static String pred_test_start;
		private static String days_history;
		private static String type_map_file;
		private static String map_file;
				
		private Map<String,Map<String,String>> info_map_table = new HashMap<String,Map<String,String>>();
		private Map<String,String> type_price_map = new HashMap<String,String>();

		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,apppv,pv");
			pred_train_start = context.getConfiguration().get("pred_train_start");
			//if pred_test_start set to no, then don't generate test set
			pred_test_start = context.getConfiguration().get("pred_test_start");	
			days_history = context.getConfiguration().get("history_days", "7:0.8,15:0.9,30:0.95,60:0.975");
			
			map_file = context.getConfiguration().get("SpecView");
			type_map_file = context.getConfiguration().get("specSaleTime");
			
			BufferedReader b =new BufferedReader(new FileReader(new File(type_map_file)));
			String str = b.readLine();
			while(str != null){
				String[] tmp = str.split("\t");
				if(tmp.length <1)
					continue;
				type_price_map.put(tmp[0], tmp[1]);
				str = b.readLine();
			}
			
			BufferedReader br =new BufferedReader(new FileReader(new File(map_file)));
			str = br.readLine();
			while(str != null){
				String[] tmp = str.split("\t");
				if(tmp.length <1)
					continue;
				Map<String,String> map = new HashMap<String,String>();
				map.put("brand", tmp[4]);
				map.put("level", tmp[6]);
				map.put("ifsale", tmp[12]);
				map.put("makeInChina", tmp[14]);
				map.put("place", tmp[19]);
				map.put("state", tmp[46]);
				info_map_table.put(tmp[0], map);
				str = br.readLine();
			}
			br.close();
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
				long days_train = diff/(1000*60*60*24);
				long days_test = 999;
				String series="";
				String spec="";
				String cookieTime = null;
				String clickTime = null;
				if(! pred_test_start.equals("no"))
				{
					d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_test_start.replaceAll("/", ""));
					diff = d2.getTime() - d.getTime();
					days_test = diff/(1000*60*60*24);
				}
				
				if(pvList != null && pvList.size() > 0)
				{
					HashMap<String, Double> dc = new HashMap<String, Double>();
					for(PvlogOperation.AutoPVInfo pvinfo : pvList) {
						
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

						String[] days = days_history.split(",");
						double maxday = Double.valueOf(days[days.length-1].split(":",2)[0]);
						if( (days_train > 0) && (days_train <= maxday) ){
							dc.put("tr_province" + "@" + province, 1.0);
							dc.put("tr_city" + "@" + city, 1.0);
						}
						if( days_test <= maxday ){
							dc.put("te_province" + "@" + province, 1.0);
							dc.put("te_city" + "@" + city, 1.0);
						}
						
						for(String part : days_history.split(","))
						{
							String brand = null;
							String level = null;
							String state = null;
							String place = null;
							String ifsale = null;
							String makeInChina = null;
							//String series_price = null;
							if(info_map_table.containsKey(series) && info_map_table.get(series).size()>0){
								brand = info_map_table.get(series).get("brand");
								level = info_map_table.get(series).get("level");
								state = info_map_table.get(series).get("state");
								place = info_map_table.get(series).get("place");
								ifsale = info_map_table.get(series).get("ifsale");
								String tmp = info_map_table.get(series).get("makeInChina");
								if(tmp.equals("国产")){
									makeInChina = String.valueOf(1);
								}else{
									makeInChina = String.valueOf(0);
								}
							
							}
							int day = Integer.valueOf(part.split(":",2)[0]);
							double decay = Double.valueOf(part.split(":",2)[1]);
							if( (days_train > 0) && (days_train <= day) )
							{
								double score = Math.pow(decay,days_train);
								if(!pvinfo.getSeriesid().equals("0"))
								{
									addFeature("tr_series_" + String.valueOf(day) + "@" + series, dc, score);
									// new feature
									addFeature("tr_clickTime_" + String.valueOf(day) + "@" + clickTime, dc, score);
									addFeature("tr_cookieTime_" + String.valueOf(day) + "@" + cookieTime, dc, score);
									addFeature("tr_brand_" + String.valueOf(day) + "@" + brand, dc, score);
									addFeature("tr_level_" + String.valueOf(day) + "@" + level, dc, score);
									//addFeature("tr_ifSale_" + String.valueOf(day) + "@" + ifsale, dc, score);
									addFeature("tr_state_" + String.valueOf(day) + "@" + state, dc, score);
									addFeature("tr_makeInChina_" + String.valueOf(day) + "@" + makeInChina, dc, score);
									addFeature("tr_place_" + String.valueOf(day) + "@" + place, dc, score);			
								}
								
								if(! pvinfo.getSpecid().equals("0"))
									add("tr_spec_"+ String.valueOf(day) + "@" + spec, dc, score);																								
								if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
									add("tr_channel_" + String.valueOf(day) + "@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id(), dc, score);
								
								// new feature for reduce calculate PVRatio
								if(! pvinfo.getSeriesid().equals("0")||! pvinfo.getSpecid().equals("0"))
									add("tr_SeriesSpecPVRatio_" + String.valueOf(day)+"@" + "on", dc, score);
								if( pvinfo.getSeriesid().equals("0") && pvinfo.getSpecid().equals("0"))
									add("tr_SeriesSpecPVRatio_" + String.valueOf(day)+"@" + "off", dc, score);
								if(!pvinfo.getSeriesid().equals("0"))
									add("tr_SeriesPVRatio_"+ String.valueOf(day) + "@" + "on",dc,score);
								else
									add("tr_SeriesPVRatio_"+ String.valueOf(day) + "@" + "off",dc,score);
								if(!pvinfo.getSpecid().equals("0"))
									add("tr_SpecPVRatio_"+ String.valueOf(day) + "@" + "on",dc,score);
								else
									add("tr_SpecPVRatio_"+ String.valueOf(day) + "@" + "off",dc,score);
								  
							}
							if( days_test <= day )
							{
								double score = Math.pow(decay,days_test);
								if(! pvinfo.getSeriesid().equals("0"))
								{
									addFeature("te_series_" + String.valueOf(day) + "@" + series, dc, score);
//									// new feature
									addFeature("te_clickTime_" + String.valueOf(day) + "@" + clickTime, dc, score);
									addFeature("te_cookieTime_" + String.valueOf(day) + "@" + cookieTime, dc, score);
									addFeature("te_brand_" + String.valueOf(day) + "@" + brand, dc, score);
									addFeature("te_level_" + String.valueOf(day) + "@" + level, dc, score);
									//addFeature("te_ifSale_" + String.valueOf(day) + "@" + ifsale, dc, score);
									addFeature("te_state_" + String.valueOf(day) + "@" + state, dc, score);
									addFeature("te_makeInChina_" + String.valueOf(day) + "@" + makeInChina, dc, score);
									addFeature("te_place_" + String.valueOf(day) + "@" + place, dc, score);								
								}
//								
								if(! pvinfo.getSpecid().equals("0"))
									add("te_spec_"+ String.valueOf(day) + "@" + spec, dc, score);
								if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
									add("te_channel_" + String.valueOf(day) + "@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id(), dc, score);
								
								// new feature for reduce calculate PVRatio
								if(! pvinfo.getSeriesid().equals("0")||! pvinfo.getSpecid().equals("0"))
									add("te_SeriesSpecPVRatio_" + String.valueOf(day)+"@" + "on", dc, score);
								if( pvinfo.getSeriesid().equals("0") && pvinfo.getSpecid().equals("0"))
									add("te_SeriesSpecPVRatio_" + String.valueOf(day)+"@" + "off", dc, score);
								if(! pvinfo.getSeriesid().equals("0"))
									add("te_SeriesPVRatio_" + String.valueOf(day)+"@" + "on",dc,score);
								else
									add("te_SeriesPVRatio_" + String.valueOf(day)+"@" + "off",dc,score);
								if(!pvinfo.getSpecid().equals("0"))
									add("te_SpecPVRatio_" + String.valueOf(day)+"@" + "on",dc,score);
								else
									add("te_SpecPVRatio_" + String.valueOf(day)+"@" + "off",dc,score);
								
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
		private Map<String,Double> serie_price_map = new HashMap<String,Double>();		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			String spec_price_map_file = context.getConfiguration().get("spec_price");
			String serie_price_map_file = context.getConfiguration().get("seriesPrice");
			add2dict(spec_price_map_file,spec_price_map);
			add2dict(serie_price_map_file,serie_price_map);
			
		}
		
		private void string2dict(String str, HashMap<String, Double> dic, HashSet<String> types) {
			if(str == null)
				return;
			String key = null;
			double val;
			String[] tmp = str.trim().split("\t");
			for(int i = 0; i < tmp.length; i ++ )
			{
				key = tmp[i].split(":",2)[0];			
				val = Double.parseDouble(tmp[i].split(":",2)[1]);
				if(key.indexOf("@") != -1)
				{
					types.add(key.split("@")[0]+"@");
				}
				if(dic.containsKey(key)) {
					dic.put(key, dic.get(key) + val);
				}
				else {
					dic.put(key, val);
				}
			}
		}
		
		private List<Entry<String, Double>> filter_sort_dict(String filter, HashMap<String, Double> dic) {
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
			    	//降序排列 
			        return o2.getValue().compareTo(o1.getValue()); 
			    }
			});
			
			return dic_lst;			
		}
		
		private Map<String, Double> filter_from_dict(String filter, HashMap<String, Double> dic) {
			HashMap<String, Double> subset = new HashMap<String, Double>();
			for(Map.Entry<String, Double> entry : dic.entrySet()) {
				if(entry.getKey().trim().indexOf(filter) != -1)
				{
					subset.put(entry.getKey(), entry.getValue());
				}
			 }			
			
			return subset;			
		}
		
		private Map<String, Double> filter_from_dict(HashMap<String, Double> dic) {
			HashMap<String, Double> subset = new HashMap<String, Double>();
			for(Map.Entry<String, Double> entry : dic.entrySet()) {
				String key = entry.getKey();
				String type = key.trim();
				// 过滤掉计算统计量的特征
				if(type.contains("PVRatio")||type.contains("series") ||type.contains("spec")
						||type.contains("channel")||type.contains("level")||type.contains("brand"))
					continue;
				else
					subset.put(key, entry.getValue());
			 }			
			
			return subset;			
		}
		
		private HashMap<String, Double> ratio_features(String prefix, List<Map.Entry<String, Double>> sort_lst, 
				Map<String,Double> spec_price_map) {
			HashMap<String, Double> new_feas = new HashMap<String, Double>();
			
			double ratio_top1 = 0.0, ratio_top3 = 0.0, sum = 0.0, spec_price_mean = 0.0, spec_price_var = 0.0,
				   series_price_mean = 0.0, series_price_var = 0.0;
			int cnt = 0, spec_cnt_var = 0, spec_sum_var = 0,series_cnt_var = 0, series_sum_var = 0;
			boolean do_price = false;
			// spec price flag
			if( sort_lst.get(0).getKey().indexOf("spec") != -1  || sort_lst.get(0).getKey().indexOf("series") != -1)
				do_price = true;
			
			
			/*
			 * 用户浏览车型车系信息比例
			 *  
			 * */
			if(prefix.indexOf("PVRatio") != -1){
				double SeriesSpecRatio_on=0;
				double SeriesSpecRatio_off=0;
				for (int i = 0; i < sort_lst.size(); i++) {
					if(sort_lst.get(i).getKey().contains("on"))
						SeriesSpecRatio_on = sort_lst.get(i).getValue();
					if(sort_lst.get(i).getKey().contains("off"))
						SeriesSpecRatio_off = sort_lst.get(i).getValue();
				}
			
				if((int)SeriesSpecRatio_on!=0||(int)SeriesSpecRatio_off!=0){
					new_feas.put(prefix ,SeriesSpecRatio_on/(SeriesSpecRatio_on+SeriesSpecRatio_off));
				}
				return new_feas;
			}
			
			for (int i = 0; i < sort_lst.size(); i++) {
			    if(i == 0)
			    	ratio_top1 += sort_lst.get(i).getValue();
			    
			    if(i > 10)  //去掉map端低频兴趣,保留map端高频兴趣
			    	break;
			    new_feas.put(sort_lst.get(i).getKey() , sort_lst.get(i).getValue());
			    
			    if(i<3)
			    {
			    	ratio_top3 += sort_lst.get(i).getValue();
			    	if(do_price)
			    	{
				    	if(spec_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1])) {
				    		spec_price_mean += Double.valueOf(spec_price_map.get(sort_lst.get(i).getKey().split("@")[1]));
				    		spec_cnt_var ++;
				    	}
				    	if(serie_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1])) {
				    		series_price_mean += Double.valueOf(serie_price_map.get(sort_lst.get(i).getKey().split("@")[1]));
				    		series_cnt_var ++;
				    	}
				    	
			    	}
			    		
			    }
			    sum += sort_lst.get(i).getValue();
			    if(sort_lst.get(i).getValue() > 0.1)
			    	cnt++;			    
			}
			if(sum > 0) {
				ratio_top1 = ratio_top1 / sum;
				ratio_top3 = ratio_top3 / sum;				
			}
			String prefix_trim = prefix.split("@")[0];
			new_feas.put(prefix_trim + "_Ratio1" , ratio_top1);
			new_feas.put(prefix_trim + "_Ratio3" , ratio_top3);
			new_feas.put(prefix_trim + "_Cnt" , (double) cnt);
			
			if(do_price)
			{
				if(spec_cnt_var > 0)
					spec_price_mean /= spec_cnt_var;
				for (int i = 0; i < sort_lst.size(); i++) {
					if(i>=3)
						break;
					if(spec_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1]))
						spec_sum_var += Math.pow(Double.valueOf(spec_price_map.get(sort_lst.get(i).getKey().split("@")[1])) - spec_price_mean, 2);				
				}
				if(spec_cnt_var > 0)
				{
					spec_price_var = Math.sqrt(spec_sum_var/spec_cnt_var);
					new_feas.put(prefix_trim + "_Mean" , spec_price_mean);
					new_feas.put(prefix_trim + "_Var" , spec_price_var);
				}
				
				if(series_cnt_var > 0)
					series_price_mean /= series_cnt_var;
				for (int i = 0; i < sort_lst.size(); i++) {
					if(i>=3)
						break;
					if(serie_price_map.containsKey(sort_lst.get(i).getKey().split("@")[1]))
						series_sum_var += Math.pow(Double.valueOf(serie_price_map.get(sort_lst.get(i).getKey().split("@")[1])) - series_price_mean, 2);				
				}
				if(series_cnt_var > 0)
				{
					series_price_var = Math.sqrt(series_sum_var/series_cnt_var);
					new_feas.put(prefix_trim + "_Mean" , series_price_mean);
					new_feas.put(prefix_trim + "_Var" , series_price_var);
				}
			}
										
			return new_feas;
		}
		
		
		private String output_map(HashMap<String, Double> map) {
			StringBuilder sb = new StringBuilder();
			java.text.DecimalFormat df = new java.text.DecimalFormat("#.00");  
			int i = 0;
			for(Map.Entry<String, Double> entry : map.entrySet()) {
				if(i > 0)
					sb.append("\t");
				i++;
				sb.append(entry.getKey());
				sb.append(":");
				double val = entry.getValue();

				sb.append(df.format(val));				
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
			 * 用户是否决定购买某款车型 还是在选择多款车型的阶段 统计量特征
			 *  
			 * */
			dic_tmp = new HashMap<String, Double>();
			for(String type : types)
			{
				//选择要算统计量的特征 
				if(type.contains("series") ||type.contains("spec")||type.contains("PVRatio")||type.contains("channel")
						||type.contains("level")||type.contains("brand")	){
					sort_lst = filter_sort_dict(type, dic);
					dic_add = ratio_features(type, sort_lst, spec_price_map);
					dic_tmp.putAll(dic_add);
					dic_add.clear();
				}
			}
			//add map端其他特征
			dic_tmp.putAll(filter_from_dict(dic));

			

			if(!dic.isEmpty())			
			    context.write(key, new Text(output_map(dic_tmp)));
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