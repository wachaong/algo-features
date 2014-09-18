package com.autohome.adrd.algo.click_model.feature_engineering.mechanism;

import java.util.Scanner;
import java.io.File;
import java.util.ArrayList;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.utility.MyPair;

public class FunctionTransformer implements Transformer {
	private ArrayList<MyPair<String, OneVarFunction>> function_list 
				= new ArrayList<MyPair<String, OneVarFunction>>();
	
	public void setup(String filename) {
		try {
			Scanner fin = new Scanner(new File(filename));
			String feature = null;
			OneVarFunction function = null;
			while(fin.hasNext()) {
				String line = fin.nextLine();
				Scanner lin = new Scanner(line);
				feature = lin.next();
				while(lin.hasNext()) {
					function = (OneVarFunction)Class.forName(lin.next()).newInstance();
					function_list.add(new MyPair<String, OneVarFunction>(feature, function));
				}
				lin.close();			
			}
			fin.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void inplaceTransform(Sample sample) {
		
		String feature = null;
		OneVarFunction fun = null;
		for(MyPair<String, OneVarFunction> pair : function_list) {
			feature = pair.getFirst();
			fun = pair.getSecond();
			sample.setFeature(feature, fun.eval(sample.getFeature(feature)));
		}
	}
	
	public Sample transform(Sample sample_in) {
		Sample sample_out = (Sample)sample_in.clone();
		inplaceTransform(sample_out);
		return sample_out;
	}

	@Override
	public ArrayList<String> transformFeatures(ArrayList<String> features_in) {
		// TODO Auto-generated method stub
		return null;
	}

}
