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
import com.autohome.adrd.algo.protobuf.TargetingKVOperation;

/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 */

public class ChannelInterest extends AbstractProcessor {

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
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick");
			pred_date = context.getConfiguration().get("pred_date");
			decay = context.getConfiguration().getDouble("decay",0.9);
		}

		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			
			Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
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
				
				int pv_cnt = 0;
				if (pvList != null && pvList.size() != 0)
					pv_cnt = pvList.size();
				
				if(pv_cnt > 0)
				{
					for(AutoPVInfo pvinfo : pvList)
					{
						double score = Math.pow(decay,days);
						if(pattern.matcher(pvinfo.getSite1Id()).matches() && pattern.matcher(pvinfo.getSite2Id()).matches() )
							context.write(new Text(cookie), new Text("channel@" + pvinfo.getSite1Id()+"#"+pvinfo.getSite2Id()+":"+String.valueOf(score)));
					}
				}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
																		
		}
	}

	public static class HReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String , Double> map = new HashMap<String , Double>(); 
			for (Text value : values) {
				String [] segs = value.toString().split(":");
				if(map.containsKey(segs[0]))
				{
					double temp = map.get(segs[0]) + Double.valueOf(segs[1]);
					map.put(segs[0], temp);
				}
				else
					map.put(segs[0], Double.valueOf(segs[1]));			
			}
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for (Map.Entry<String, Double> entry : map.entrySet())
			{
				if(i > 0)
					sb.append("\t");
				i++;
				sb.append(entry.getKey());
				sb.append(":");
				sb.append(entry.getValue());
				//sb.append(",");
			}
			
			context.write(key, new Text(sb.toString()));
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