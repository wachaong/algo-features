package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;


import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.protobuf.AdLogOldOperation;

public class GenerateBaseSample extends AbstractProcessor {
		
	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_ADDISPLAY = "adoldpv";
		public static final String CG_CLK = "adoldclk";
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection","adoldclk,adoldpv");
		}

		@SuppressWarnings("unchecked")
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			List<AdLogOldOperation.AdPVOldInfo> pvList = new ArrayList<AdLogOldOperation.AdPVOldInfo>();
			List<AdLogOldOperation.AdCLKOldInfo> clkList = new ArrayList<AdLogOldOperation.AdCLKOldInfo>();
			decode(key, value);

			HashMap<String, Double> dc = new HashMap<String, Double>();
			List<String> PVID = new ArrayList<String>();
			
			pvList = (List<AdLogOldOperation.AdPVOldInfo>) list.get(CG_ADDISPLAY);
			clkList = (List<AdLogOldOperation.AdCLKOldInfo>) list.get(CG_CLK);
			
			if (clkList != null && clkList.size() != 0) {
				PVID.clear();
				for(AdLogOldOperation.AdCLKOldInfo clkinfo : clkList)
				{
					PVID.add(clkinfo.getPvid());
				}
			}
			
				
			if(pvList != null && pvList.size() > 0)
			{
				
				for(AdLogOldOperation.AdPVOldInfo pvinfo: pvList) {
					dc.clear();
					String cookie = pvinfo.getCookie().trim();
					if(cookie.length() > 36)
						cookie = cookie.substring(0, 35);
					//String creativeid = pvinfo.getCreativeid().trim();
					
					//String pageid = pvinfo.getPageid().trim();
					//String psid = pvinfo.getPsid().trim();
					
					//String regionid = pvinfo.getRegionid().trim();
					//String vtime = pvinfo.getVtime().trim();
					
					//String carid = pvinfo.getCarid().trim();
					String pvid = pvinfo.getPvid().trim();
					//String reqid = pvinfo.getReqid().trim();
					
					//dc.put("cookie" + "@" + cookie, 1.0);
					//dc.put("creativeid" + "@" + creativeid, 1.0);
					//dc.put("vtime" + "@" + vtime, 1.0);
					//dc.put("pvid" + "@" + pvid, 1.0);
					String rst = "cookie" + "@" + cookie + "\t" + "pvid" + "@" + pvid;
					if(cookie != null  && !cookie.isEmpty()){
						if(PVID.contains(pvid))
							context.write(new Text("1"), new Text(rst));
						else
							context.write(new Text("0"), new Text(rst));
					}
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