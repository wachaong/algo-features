package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


import java.util.Map;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.autohome.adrd.algo.click_model.io.AbstractProcessor;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;
import com.autohome.adrd.algo.sessionlog.consume.RCFileBaseMapper;
/**
 * 
 * @author [Chen shuaihua ]
 * 
 *输出训练集、测试集label和historysale特征
 * 
 */
public class HistroySale_Label extends AbstractProcessor {

	public static class RCFileMapper extends RCFileBaseMapper<Text, Text> {

		public static final String CG_USER = "user";
		public static final String CG_PV = "pv";
		public static final String CG_SEARCH = "search";
		public static final String CG_SALE_LEADS = "saleleads";
		public static final String CG_TAGS = "tags";
		public static final String CG_APPPV = "apppv";
		public static final String CG_BEHAVIOR = "behavior";
		private static String pred_test_start;
		private static String pred_train_start;
		private static String days_history;
		private static String pred_days;
		
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			projection = context.getConfiguration().get("mapreduce.lib.table.input.projection", "user,addisplay,adclick,pv,apppv,saleleads,tags");
			pred_train_start = context.getConfiguration().get("pred_train_start");
			//if pred_test_start set to no, then don't generate test set
			pred_test_start = context.getConfiguration().get("pred_test_start");	
			days_history = context.getConfiguration().get("history_days", "7:0.8,15:0.9,30:0.95,60:0.975");	
			pred_days = context.getConfiguration().get("pred_days");
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


		@SuppressWarnings({ "unchecked", "deprecation" })
		public void map(LongWritable key, BytesRefArrayWritable value, Context context) throws IOException, InterruptedException {
			
			List<SaleleadsInfoOperation.SaleleadsInfo> saleleadsList = new ArrayList<SaleleadsInfoOperation.SaleleadsInfo>();
			List<PvlogOperation.AutoPVInfo> pvList = new ArrayList<PvlogOperation.AutoPVInfo>();
			decode(key, value);

			saleleadsList = (List<SaleleadsInfoOperation.SaleleadsInfo>) list.get(CG_SALE_LEADS);
			pvList = (List<PvlogOperation.AutoPVInfo>) list.get(CG_PV);
			String cookie = (String) list.get("user");
			String path=((FileSplit)context.getInputSplit()).getPath().toString();
			String date_sale ="";
			String date_pv ="";	
			if(path.split("/sessionlog_sale/").length==2)
				date_sale = path.split("/sessionlog_sale/")[1].split("part")[0].replaceAll("/", "");
			if(path.split("/sessionlog/").length==2)
				date_pv = path.split("/sessionlog/")[1].split("part")[0].replaceAll("/", "");
			
			Date d_sale;
			Date d_pv;
			

			
			//sessionlog_sale
			
			try {
				HashMap<String, Double> dc = new HashMap<String, Double>();
				d_sale = new SimpleDateFormat("yyyyMMdd").parse(date_sale);
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_train_start.replaceAll("/", ""));
				long diff = d2.getTime() - d_sale.getTime();
				long days_train_sale = diff/(1000*60*60*24);
				long days_test_sale = 999;
				
				
				
			


				
				if(! pred_test_start.equals("no"))
				{
					d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_test_start.replaceAll("/", ""));
					diff = d2.getTime() - d_sale.getTime();
					days_test_sale = diff/(1000*60*60*24);
					
				}
				
				
				if (saleleadsList != null && saleleadsList.size() != 0 )
				{
					
				
					// 生成训练集 测试集 history 特征
					for(String part : days_history.split(","))
					{
						int day = Integer.valueOf(part.split(":",2)[0]);
						double decay = Double.valueOf(part.split(":",2)[1]);
						if( (days_train_sale > 0) && (days_train_sale <= day) )
						{
							double score = Math.pow(decay,days_train_sale);
							add("tr_hissaleleads_" + String.valueOf(day), dc, score);								
						}
						if( (days_test_sale > 0) &&(days_test_sale <= day) )
						{
							double score = Math.pow(decay,days_test_sale);
							add("te_hissaleleads_" + String.valueOf(day), dc, score);								
						}																											
					}
					
					if(!dc.isEmpty()){
						context.write(new Text(cookie), new Text(output_map(dc)));
					}
    
					
				
					if( ((int)days_train_sale <= 0) && ((int)days_train_sale >= Integer.parseInt(pred_days)*(-1)) )
					{

						context.write(new Text(cookie), new Text("tr_sale"));
	
						
					}
					if( ((int)days_test_sale <= 0) && ((int)days_test_sale >= Integer.parseInt(pred_days)*(-1)) )
					{
						
						context.write(new Text(cookie), new Text("te_sale"));
				
					
					}
					
					
					
					   
				}
					
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//sessionlog
			
			try {
				HashMap<String, Double> dc = new HashMap<String, Double>();
				
				
				Date d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_train_start.replaceAll("/", ""));
				d_pv = new SimpleDateFormat("yyyyMMdd").parse(date_pv);
				long  diff = d2.getTime() - d_pv.getTime();
				long days_train_pv = diff/(1000*60*60*24);
				long days_test_pv = 999;

		
				
				if(! pred_test_start.equals("no"))
				{
					d2 = new SimpleDateFormat("yyyyMMdd").parse(pred_test_start.replaceAll("/", ""));
					
					
					diff = d2.getTime() - d_pv.getTime();
					days_test_pv = diff/(1000*60*60*24);
				}
				


				
				if(pvList != null && pvList.size() > 0 )
				{

					
					if( ((int)days_train_pv <= 0) && ((int)days_train_pv >= Integer.parseInt(pred_days)*(-1)) )
					{

						context.write(new Text(cookie), new Text("tr_pv"));
					
					}
					if( ((int)days_test_pv <= 0) && ((int)days_test_pv >= Integer.parseInt(pred_days)*(-1)) )
					{
						
						context.write(new Text(cookie), new Text("te_pv"));
			
					
					}
					
				}
				
				
				
					
				
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

																		
		}
	}

	public static class HReduce extends Reducer<Text, Text, Text, Text> {
		
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

		private void add(String fea, HashMap<String, Double> map, double score) {
			if(map.containsKey(fea)) {
				map.put(fea, map.get(fea) + score);
			}
			else
				map.put(fea, score);	
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int tr_sale =0;
			int tr_pv=0;
			int te_sale =0;
			int te_pv =0;
			HashMap<String,Double> featurevalue = new HashMap<String,Double>();
			for (Text value : values) {
				String  seg = value.toString();
				if(seg.contains("tr_sale"))
					tr_sale=1;
				if(seg.contains("tr_pv"))
					tr_pv=1;
				if(seg.contains("te_sale"))
					te_sale=1;
				if(seg.contains("te_pv"))
					te_pv=1;
				if(seg.contains("hissaleleads")){
					String[] hissales = seg.split("\t");
					for(int i=0;i<hissales.length;++i){
						String[] hissale = hissales[i].split(":");
						add(hissale[0],featurevalue,Double.valueOf(hissale[1]));
					}

				}
			
			
			}
			
			if(tr_sale==1)
				context.write(key,new Text("tr_label"+":1"));
			else if(tr_pv==1 )
				context.write(key,new Text("tr_label"+":0"));
			
			
			if(te_sale==1)
				context.write(key,new Text("te_label"+":1"));
			else if(te_pv==1)
				context.write(key,new Text("te_label"+":0"));
			

			if(!featurevalue.isEmpty())
				context.write(key,new Text(output_map(featurevalue)));
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
