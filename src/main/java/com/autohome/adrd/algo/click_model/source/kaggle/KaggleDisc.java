package com.autohome.adrd.algo.click_model.source.kaggle;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.feature_engineering.mechanism.Transformer;

public class KaggleDisc implements Transformer {
	
	private Map<String, String>  featuredisc = null;

	@Override
	public void setup(String conf_path) {
		featuredisc =  readMapBySep(conf_path, "utf8", 2);
		
	}

	@Override
	public void inplaceTransform(Sample input_sample) {
		for(Map.Entry<String, Double> ent : input_sample.getFloatFeatures().entrySet()) {
			int i = Integer.parseInt(ent.getKey().substring(1));
			double value = ent.getValue();
			String s = featuredisc.get((i-2));
			if(s == null)
				continue;
			String[] bucket = featuredisc.get((i-2)+"").split("@");
			for(int id=0;id<bucket.length;id++){
				String[] valueid= bucket[id].split("#");
				if(value < Integer.parseInt(valueid[0])) {
					input_sample.getIdFeatures().add(valueid[1]);
				}
			}
		}
		
		input_sample.getFloatFeatures().clear();
		
	}

	@Override
	public Sample transform(Sample input_sample) {
		Sample output_sample = (Sample)input_sample.clone();
		inplaceTransform(output_sample);
		return output_sample;
	}

	@Override
	public ArrayList<String> transformFeatures(ArrayList<String> features_in) {
		ArrayList<String> features_out = new ArrayList<String>();
		for(String fea : features_in) {
			if(!fea.startsWith("I"))
				features_out.add(fea);
		}
		
		for(Integer i = 1; i < 293; ++i) {
			features_out.add(i.toString());
		}
		
		return features_out;
	}
	
	private Map<String, String> readMapBySep(String fileName,
			String encoding, int sepPos) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(fileName), encoding));
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#") || line.trim().isEmpty()) {
					continue;
				}
				String[] tokens = line.split("\t", sepPos);
				map.put(tokens[0], tokens[1]);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}
	

}
