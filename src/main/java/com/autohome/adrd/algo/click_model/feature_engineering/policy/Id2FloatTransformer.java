package com.autohome.adrd.algo.click_model.feature_engineering.policy;


import java.io.File;
import java.util.Scanner;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.ArrayList;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.feature_engineering.mechanism.Transformer;

/**
 * for CTR,... tansform
 * ctr
 * Male\t0.004
 * Female\t0.005
 * @author Mingmin Yang
 *
 */
public class Id2FloatTransformer implements Transformer {
	private HashMap<String, Double> replace_feature = new HashMap<String, Double>();
	private String postfix = null;
	
	public void setup(String file_path) {
		try {
			Scanner fin = new Scanner(new File(file_path));
			postfix = fin.next();
			String feature = null;
			double value;
			int i = 0;
			while(fin.hasNext()) {
				i++;
				feature = fin.next();
				value = fin.nextDouble();
				replace_feature.put(feature, value);
			}
			
			fin.close();
		} catch(IOException ex) {
			ex.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void inplaceTransform(Sample sample) {
		Iterator<Map.Entry<String, Double>> iter = replace_feature.entrySet().iterator();
		Map.Entry<String, Double> entry = null;
		String feature = null;
		double value;
		while(iter.hasNext()) {
			entry = iter.next();
			feature = entry.getKey();
			value = entry.getValue();
			String group_name = feature.split("@")[0];
			if(sample.getIdFeatures().contains(feature)) {
				sample.getIdFeatures().remove(feature);
				sample.setFeature(group_name +"." +  postfix, value);
			}	
		}
	}
	
	public Sample transform(Sample sample_in) {
		Sample sample_out = (Sample)sample_in.clone();
		inplaceTransform(sample_out);   		
		return sample_out;
	}
	
	public ArrayList<String> transformFeatures(ArrayList<String> features_in) {
		ArrayList<String> features_out = new ArrayList<String>();
		
		for(String feature : features_in) {
			if(replace_feature.containsKey(feature)) {
				String group_name = feature.split("@")[0];
				features_out.add(group_name + "." + postfix);
			}
			else
				features_out.add(feature);
		}
		
		return features_out;
		
	}

}
