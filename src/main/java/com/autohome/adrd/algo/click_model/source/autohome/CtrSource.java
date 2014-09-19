package com.autohome.adrd.algo.click_model.source.autohome;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.driver.SampleGeneratorHelper;
import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.AdLogOperation;


/**
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 * 
 */

public class CtrSource extends AbstractProcessor {
	
	public static class RCFileMapper extends RCFileBaseMapper<NullWritable, Sample> {
		
		public static final String CG_USER = "user";
		public static final String CG_ADDISPLAY = "addisplay";
		public static final String CG_CLK = "adclick";
		private SampleGeneratorHelper helper = new SampleGeneratorHelper();
			
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick");
			String config_file = context.getConfiguration().get("config_file");
			helper.setup(config_file);
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
					Sample s = new Sample();
					s.setFeature("creativeid@" + clkinfo.getCreativeid());
					s.setFeature("adtype@" + clkinfo.getAdtype());
					s.setFeature("psid@" + clkinfo.getPsid());
					s.setFeature("regionid@" + clkinfo.getRegionid());
					s.setLabel(1.0);
					Sample sample_out = helper.process2(s);
					
					context.write( NullWritable.get(), sample_out);
				}
			}
			
			if (pvList != null && pvList.size() != 0) {
				for(AdLogOperation.AdPVInfo pvinfo : pvList)
				{
					
					fea_lst = pvinfo.getCreativeid() + "\t" + pvinfo.getAdtype() + "\t" + pvinfo.getPsid() + "\t"
							+ pvinfo.getRegionid();
					
					if(! clk_set.contains(pvinfo.getPsid() + "," + pvinfo.getCreativeid()))
					{
						Sample s = new Sample();
						s.setFeature("creativeid@" + pvinfo.getCreativeid());
						s.setFeature("adtype@" + pvinfo.getAdtype());
						s.setFeature("psid@" + pvinfo.getPsid());
						s.setFeature("regionid@" + pvinfo.getRegionid());
						s.setLabel(0.0);
						Sample sample_out = helper.process2(s);
						context.write( NullWritable.get(), sample_out);
					}					
				}
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