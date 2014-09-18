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


/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 */

public class LabelInstanceSingleOld extends AbstractProcessor {
	
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {
		
		public static final String CG_USER = "user";
		public static final String CG_ADDISPLAY = "addisplay";
		public static final String CG_CLK = "adclick";
			
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick");
		}
		
		public static StringBuilder join(Collection<?> follows, String sep) {
			StringBuilder sb = new StringBuilder();

			if (follows == null || sep == null) {
				return sb;
			}

			Iterator<?> it = follows.iterator();
			while (it.hasNext()) {
				sb.append(it.next());
				if (it.hasNext()) {
					sb.append(sep);
				}
			}
			return sb;
		}
		
		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			List<AdLogOperation.AdPVInfo> pvList = new ArrayList<AdLogOperation.AdPVInfo>();
			List<AdLogOperation.AdCLKInfo> clkList = new ArrayList<AdLogOperation.AdCLKInfo>();
			decode(key, value);
	
			pvList = (List<AdLogOperation.AdPVInfo>) list.get(CG_ADDISPLAY);
			clkList = (List<AdLogOperation.AdCLKInfo>) list.get(CG_CLK);
			
			HashSet<String> clk_set = new HashSet<String>();
			String fea_lst = "";
		
			if (clkList != null && clkList.size() != 0) {
				for(AdLogOperation.AdCLKInfo clkinfo : clkList)
				{
					fea_lst = clkinfo.getCreativeid() + "\t" + clkinfo.getAdtype() + "\t" + clkinfo.getPsid() + "\t"
							+ clkinfo.getRegionid();
					
					clk_set.add(clkinfo.getPsid() + "," + clkinfo.getCreativeid());
					
					context.write(new Text(fea_lst), new Text("1"));
				}
			}
			
			if (pvList != null && pvList.size() != 0) {
				for(AdLogOperation.AdPVInfo pvinfo : pvList)
				{
					
					fea_lst = pvinfo.getCreativeid() + "\t" + pvinfo.getAdtype() + "\t" + pvinfo.getPsid() + "\t"
							+ pvinfo.getRegionid();
					
					if(! clk_set.contains(pvinfo.getPsid() + "," + pvinfo.getCreativeid()))
						context.write(new Text(fea_lst), new Text("0"));
								
				}
			}										
		}
	}
	
	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int pv_cnt = 0, clk_cnt = 0;
			for (Text value : values) {
				if(value.toString().equals("1"))
				{
					pv_cnt += 1;
					clk_cnt += 1;
				}
				if(value.toString().equals("0"))
				{
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