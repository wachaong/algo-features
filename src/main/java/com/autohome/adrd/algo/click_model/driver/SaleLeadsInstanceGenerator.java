package com.autohome.adrd.algo.click_model.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.driver.CtrInstanceGenerator.RCFileMapper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.autohome.adrd.algo.protobuf.ApplogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;
import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;

public class SaleLeadsInstanceGenerator extends AbstractProcessor {
	
	public static class RCFileMapper extends RCFileBaseMapper<NullWritable, Sample> {
		
		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";
		public static final String CG_SEARCH = "search";
		public static final String CG_SALE_LEADS = "saleleads";
		public static final String CG_TAGS = "tags";
		public static final String CG_APPPV = "apppv";
		public static final String CG_BEHAVIOR = "behavior";
		private SampleGeneratorHelper helper = new SampleGeneratorHelper();
			
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick,pv,apppv,saleleads,tags");
			String config_file = context.getConfiguration().get("config_file");
			helper.setup(config_file);
		}
				
		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			List<ApplogOperation.AutoAppInfo> app_pvList = new ArrayList<ApplogOperation.AutoAppInfo>();
			List<SaleleadsInfoOperation.SaleleadsInfo> saleleadsList = new ArrayList<SaleleadsInfoOperation.SaleleadsInfo>();

			decode(key, value);

			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			app_pvList = (List<ApplogOperation.AutoAppInfo>) list.get(CG_APPPV);
			saleleadsList = (List<SaleleadsInfoOperation.SaleleadsInfo>) list.get(CG_SALE_LEADS);

			TargetingKVOperation.TargetingInfo targeting = (TargetingKVOperation.TargetingInfo) list.get(CG_TAGS);
			
			HashSet<String> clk_set = new HashSet<String>();
		
			Sample pos = new Sample();
			Sample neg = new Sample();
			pos.setLabel(1.0);
			neg.setLabel(0.0);
			if (targeting != null) {
				if (targeting.getSeriesListCount() != 0) {
					for (int i = 0; i < targeting.getSeriesListList().size(); i++) {

						pos.setFeature("targetingSeriesid@"+targeting.getSeriesListList().get(i).getSeriesid(), targeting.getSeriesListList().get(i).getScore());
						neg.setFeature("targetingSeriesid@"+targeting.getSeriesListList().get(i).getSeriesid(), targeting.getSeriesListList().get(i).getScore());
						
						
					}
				}
//				if (targeting.getBrandListCount() != 0) {
//					for (int i = 0; i < targeting.getBrandListList().size(); i++) {
//						if(targeting.getBrandListList().get(i).getBrandid()!=null&&targeting.getBrandListList().get(i).getScore()+""!="" ){
//							pos.setFeature("targetingBrandid@"+targeting.getBrandListList().get(i).getBrandid(), targeting.getBrandListList().get(i).getScore());
//							neg.setFeature("targetingBrandid@"+targeting.getBrandListList().get(i).getBrandid(), targeting.getBrandListList().get(i).getScore());
//							
//						}
//					
//						
//					}
//				}
//				if (targeting.getSpecListCount() != 0) {
//				for (int i = 0; i < targeting.getSpecListList().size(); i++) {
//		
//				
//					pos.setFeature("targetingSpecid@"+targeting.getSpecListList().get(i).getSpecid(), targeting.getSpecListList().get(i).getScore());
//					neg.setFeature("targetingSpecid@"+targeting.getSpecListList().get(i).getSpecid(), targeting.getSpecListList().get(i).getScore());
//					
//					
//				}
//			}
//			if (targeting.getLevelListCount() != 0) {
//				for (int i = 0; i < targeting.getLevelListList().size(); i++) {
//	
//
//					pos.setFeature("targetingLevelid@"+targeting.getLevelListList().get(i).getLevelid(), targeting.getLevelListList().get(i).getScore());
//					neg.setFeature("targetingLevelid@"+targeting.getLevelListList().get(i).getLevelid(), targeting.getLevelListList().get(i).getScore());
//				}
//			}
//			if (targeting.getPriceListCount() != 0) {
//				for (int i = 0; i < targeting.getPriceListList().size(); i++) {
//
//
//					pos.setFeature("targetingPriceid@"+targeting.getPriceListList().get(i).getPriceid(), targeting.getPriceListList().get(i).getScore());
//					neg.setFeature("targetingPriceid@"+targeting.getPriceListList().get(i).getPriceid(), targeting.getPriceListList().get(i).getScore());
//				}
//			}
//			
//			if (targeting.getExtendInfoListCount() != 0) {
//				for (int i = 0; i < targeting.getExtendInfoListList().size(); i++) {
//					pos.setFeature("targetingTagid@"+targeting.getExtendInfoListList().get(i).getTagid(), targeting.getExtendInfoListList().get(i).getScore());
//					neg.setFeature("targetingTagid@"+targeting.getExtendInfoListList().get(i).getTagid(), targeting.getExtendInfoListList().get(i).getScore());
//					
//				}
//			}
				
			}
			
			HashSet<String> saleleads_set = new HashSet<String>();
			
			
			if (saleleadsList != null && saleleadsList.size() != 0) {
				for (SaleleadsInfoOperation.SaleleadsInfo saleleads : saleleadsList) {
					
					if(saleleads_set.contains(saleleads.getCururl() + "\t" + saleleads.getRefurl()))
						continue;
					else
						saleleads_set.add(saleleads.getCururl() + "\t" + saleleads.getRefurl());
					
				//	context.write(new Text(tags), new Text("1"));
					context.write( NullWritable.get(), pos);
				}
			} else if (pvList != null && pvList.size() != 0) {
				for (PvlogOperation.AutoPVInfo pvinfo : pvList) {

				}

				//context.write(new Text(tags), new Text("0"));
				context.write( NullWritable.get(), neg);
			} else if (app_pvList != null && app_pvList.size() != 0) {
				for (ApplogOperation.AutoAppInfo apppvinfo : app_pvList) {

				}
				
				//context.write(new Text(tags), new Text("0"));
				context.write( NullWritable.get(), neg);
			}
		
			
			
									
		}
	}
	
	
	@Override
	protected void configJob(Job job) {
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(RCFileMapper.class);
		job.setMapOutputValueClass(Sample.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Sample.class);
		job.setOutputKeyClass(NullWritable.class);
	}	
}
