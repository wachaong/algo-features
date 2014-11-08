package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.ApplogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;
import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;

public class SaleLeadsLabelTest extends AbstractProcessor {

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
			String cookie = (String)list.get(CG_USER);

			StringBuilder sb = new StringBuilder();
			if (pvList != null && pvList.size() != 0) //only pc and m
			{				
				if (saleleadsList != null && saleleadsList.size() != 0) 
				{
					context.write(new Text(cookie), new Text("label1"+":"+saleleadsList.size()));
				} 
				else
				{
					context.write(new Text(cookie), new Text("label0"+":0"));					
				} 
			}
			else if(app_pvList != null && app_pvList.size() != 0)
			{
				if (saleleadsList != null && saleleadsList.size() != 0) 
				{
					context.write(new Text(cookie), new Text("label3"+":"+saleleadsList.size()));
				} 
				else
				{
					context.write(new Text(cookie), new Text("label4"+":0"));					
				} 
			}
			else
			{
				if (saleleadsList != null && saleleadsList.size() != 0) 
				{
					context.write(new Text(cookie), new Text("label5"+":"+saleleadsList.size()));
				} 
				else
				{
					context.write(new Text(cookie), new Text("label6"+":0"));					
				} 
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
