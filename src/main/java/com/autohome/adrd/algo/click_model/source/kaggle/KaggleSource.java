package com.autohome.adrd.algo.click_model.source.kaggle;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.feature_engineering.mechanism.Source;

import org.apache.hadoop.io.Text;

public class KaggleSource implements Source {
	private static final String[] names = {"Id", "Label", "I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I10", 
		"I11", "I12", "I13", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8",
		"C9", "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C18",
		"C19", "C20", "C21", "C22", "C23", "C24", "C25", "C26"}; 
	private static final int N0 = 15; 
	private static final int N = names.length;

	public Sample process(Object raw_data) {
		Sample sample = new Sample();
		String data = ((Text)raw_data).toString();
		//String data = (raw_data).toString();
		String[] tmp = data.split(",");

		if(tmp.length > 2 && !tmp[1].isEmpty()) {


			//set label
			sample.setLabel(Integer.parseInt(tmp[1]));

			for(int i = 2; i < N0; i++) {
				//to-do handle the missing values.
				if(!"".equals(tmp[i])) {
					sample.setFeature(names[i], Float.parseFloat(tmp[i].trim()));
				}
				else {
					sample.setFeature(names[i], 0.0);
				}
			}

			for(int i = N0; i < tmp.length; i++) {
				if(!"".equals(tmp[i])) {
					//sample.setFeature(names[i] + "@"  + tmp[i]);
					sample.setFeature(tmp[i]);
				}
				else {
					//sample.setFeature(names[i] + "@NA");  //not available
				}
			}
			
			//for(int i = tmp.length; i < N; i++) {
			//	sample.setFeature(names[i] + "@NA");  
			//}
		}
		return sample;
	}

}
