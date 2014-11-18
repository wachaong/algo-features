package com.autohome.adrd.algo.click_model.feature_engineering.mechanism;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.autohome.adrd.algo.click_model.data.Sample;

public class BinaryDiscretization implements Transformer {
	HashMap<String, Double> feature_thresh_map = new HashMap<String, Double>();
	
	@Override
	public void setup(String file_path) {
		Scanner in = null;
		try {
			in = new Scanner(new File(file_path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		while(in.hasNext()) {
		    feature_thresh_map.put(in.next(), in.nextDouble());
		}
	}

	@Override
	public void inplaceTransform(Sample sample) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Sample transform(Sample input_sample) {
		Sample output_sample = new Sample();
		output_sample.setLabel(input_sample.getLabel());
		String feature = null;
		double value = 0.0;
		for(String fea : input_sample.getIdFeatures()) {
			output_sample.setFeature(fea);
		}
		for(Map.Entry<String, Double> entry : input_sample.getFloatFeatures().entrySet()) {
		    feature = entry.getKey();
		    value = entry.getValue();
		    if(feature_thresh_map.containsKey(feature)) {
		    	if(value > feature_thresh_map.get(feature))
		    		output_sample.setFeature(feature + "@1");
		    	else 
		    		output_sample.setFeature(feature + "@0");
		    }
		    else {
		    	output_sample.setFeature(feature, value);
		    }
		}
		return output_sample;
	}

	@Override
	public ArrayList<String> transformFeatures(ArrayList<String> features_in) {
		// TODO Auto-generated method stub
		return null;
	}

}
