package com.autohome.adrd.algo.click_model.realtime;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.click_model.source.autohome.RawTarget.HReduce;
import com.autohome.adrd.algo.click_model.source.autohome.RawTarget.RCFileMapper;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;

public class RT_PVFeas extends AbstractProcessor {

	@Override
	protected void configJob(Job job) {
		// TODO Auto-generated method stub
		job.getConfiguration().set("mapred.job.priority", "VERY_HIGH");
		job.setMapperClass(RCFileMapper.class);
		job.setReducerClass(HReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}
	
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,pv");
		}
		
		private long getSlice(String inTime,long unit,long u){
			long slice = -1;
			try {
				   Date d = new SimpleDateFormat("yyyyMMddHHmmss").parse(inTime.replaceAll("-", "").replaceAll(":", ""));
				   slice = (d.getTime()%unit)/u;
				 }catch (ParseException e) {
					 //TODO Auto-generated catch block
			}
			return slice;
		}
		
		@SuppressWarnings("unchecked")
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			decode(key, value);

			pvList = (List<PvlogOperation.AutoPVInfo>) list.get("pv");
			for(PvlogOperation.AutoPVInfo pvinfo : pvList) {
				long slice = getSlice(pvinfo.getVisittime(),1000*60,5); //5分钟一个时间片
				Map<String,Double> feas = getFeas(pvinfo);
				
				for(long id = slice + 1; id <= slice + 12; id++)  //窗口一小时
				{
					context.write(new Text(String.valueOf(id)), new Text(feas.toString()));
				}
			}
		}				
	}
	
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for (Text value : values) {
				if(value.toString().trim().isEmpty())
					continue;
				string2dict(value.toString(), dic, types);
			}
			
		}
	}

	
}