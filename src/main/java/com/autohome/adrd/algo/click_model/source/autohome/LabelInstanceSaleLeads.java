package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.autohome.adrd.algo.protobuf.ApplogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;

/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 */

public class LabelInstanceSaleLeads extends AbstractProcessor {

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
			//projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick");
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick,pv,apppv,saleleads,tags");

		}

		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			List<ApplogOperation.AutoAppInfo> app_pvList = new ArrayList<ApplogOperation.AutoAppInfo>();
			List<SaleleadsInfoOperation.SaleleadsInfo> saleleadsList = new ArrayList<SaleleadsInfoOperation.SaleleadsInfo>();

			decode(key, value);

			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			app_pvList = (List<ApplogOperation.AutoAppInfo>) list.get(CG_APPPV);
			saleleadsList = (List<SaleleadsInfoOperation.SaleleadsInfo>) list.get(CG_SALE_LEADS);

			TargetingKVOperation.TargetingInfo targeting = (TargetingKVOperation.TargetingInfo) list.get(CG_TAGS);
			String cookie = (String) list.get(CG_USER);
			
			

			
			
			StringBuilder sb = new StringBuilder();
			if (targeting != null) {
				sb.append(cookie);
				//sb.append(targeting.getCookieTime());
//				if (targeting.getSeriesListCount() != 0) {
//					for (int i = 0; i < targeting.getSeriesListList().size(); i++) {
//						
//						sb.append(targeting.getSeriesListList().get(i).getSeriesid());
//						sb.append(":");
//						sb.append(targeting.getSeriesListList().get(i).getScore());
//						sb.append("\t");
//						
//					}
//				}
//				if (targeting.getBrandListCount() != 0) {
//					for (int i = 0; i < targeting.getBrandListList().size(); i++) {
//						
//						sb.append(targeting.getBrandListList().get(i).getBrandid());
//						sb.append(":");
//						sb.append(targeting.getBrandListList().get(i).getScore());
//						sb.append("\t");
//						
//					}
//				}
//				if (targeting.getSpecListCount() != 0) {
//					for (int i = 0; i < targeting.getSpecListList().size(); i++) {
//						sb.append(targeting.getSpecListList().get(i).getSpecid());
//						sb.append(":");
//						sb.append(targeting.getSpecListList().get(i).getScore());
//						sb.append("\t");
//					}
//				}
//				if (targeting.getLevelListCount() != 0) {
//					for (int i = 0; i < targeting.getLevelListList().size(); i++) {
//						sb.append(targeting.getLevelListList().get(i).getLevelid());
//						sb.append(":");
//						sb.append(targeting.getLevelListList().get(i).getScore());
//						sb.append("\t");
//					}
//				}
//				if (targeting.getPriceListCount() != 0) {
//					for (int i = 0; i < targeting.getPriceListList().size(); i++) {
//						sb.append(targeting.getPriceListList().get(i).getPriceid());
//						sb.append(":");
//						sb.append(targeting.getPriceListList().get(i).getScore());
//						sb.append("\t");
//					}
//				}
//				
//				if (targeting.getExtendInfoListCount() != 0) {
//					for (int i = 0; i < targeting.getExtendInfoListList().size(); i++) {
//						sb.append(targeting.getExtendInfoListList().get(i).getTagid());
//						sb.append(":");
//						sb.append(targeting.getExtendInfoListList().get(i).getScore());
//						sb.append("\t");
//					}
//				}				
		
			
			String tags = sb.toString();

			int pv_cnt = 0;
			if (pvList != null && pvList.size() != 0)
				pv_cnt += pvList.size();
			if (app_pvList != null && app_pvList.size() != 0)
				pv_cnt += app_pvList.size();

			HashSet<String> clk_set = new HashSet<String>();
			HashSet<String> saleleads_set = new HashSet<String>();
			
			String fea_lst = "";
			if (saleleadsList != null && saleleadsList.size() != 0) {
//				for (SaleleadsInfoOperation.SaleleadsInfo saleleads : saleleadsList) {
//					
//					if(saleleads_set.contains(saleleads.getCururl() + "\t" + saleleads.getRefurl()))
//						continue;
//					else
//						saleleads_set.add(saleleads.getCururl() + "\t" + saleleads.getRefurl());
//					
//					
//				}
				
				
				context.write(new Text(tags), new Text("1"));
				
				
			} else if (pvList != null && pvList.size() != 0) {
				for (PvlogOperation.AutoPVInfo pvinfo : pvList) {

				}

				context.write(new Text(tags), new Text("0"));
			} 
//			else if (app_pvList != null && app_pvList.size() != 0) {
//				for (ApplogOperation.AutoAppInfo apppvinfo : app_pvList) {
//
//				}
//				
//				context.write(new Text(tags), new Text("0"));
//			}
			
			
//			if (app_pvList != null && app_pvList.size() != 0) {
//				if (saleleadsList != null && saleleadsList.size() != 0) {
//					context.write(new Text(tags), new Text("1"));
//					
//				}else{
//					context.write(new Text(tags), new Text("0"));
//				}
//				
//				
//				
//				
//			}
			
			

			
			
			
		} 
			
			
		
		}
	}

	public static class HReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int pv_cnt = 0, clk_cnt = 0;
			for (Text value : values) {
				if (value.toString().equals("1")) {
					pv_cnt += 1;
					clk_cnt += 1;
				}
				if (value.toString().equals("0")) {
					pv_cnt += 1;
				}
			}

			context.write(new Text(String.valueOf(clk_cnt) + "\t" + String.valueOf(pv_cnt)), key);
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