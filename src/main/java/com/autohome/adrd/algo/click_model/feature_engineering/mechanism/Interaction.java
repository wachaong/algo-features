package com.autohome.adrd.algo.click_model.feature_engineering.mechanism;

import com.autohome.adrd.algo.click_model.data.Sample;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;


/**
 * Add interaction features to the sample. 
 * 
 * The format of the configure file is as follows: each line consists of two strings 
 * separated by whitespace. The two strings indicates the very two features you want to interact.
 * 
 * If a line is as
 * feature1	feature2
 * the new intersectional feature is feature1##feature2 if feature1 < feature2 or feature2##feature1 otherwise.
 * 
 * 
 * @author Mingmin Yang
 *
 */
public class Interaction implements Transformer {

	private HashSet<String> inter_feature = null;
	private final String sep = "##";


	public Interaction () {
		inter_feature = new HashSet<String>();
	}


	public void setup(String file_path) {
		Scanner in = null;
		try {
			in = new Scanner(new File(file_path));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String fea1, fea2;
		while(in.hasNext()) {
			fea1 = in.next();
			fea2 = in.next();
			if(fea1.compareTo(fea2) < 0)
				inter_feature.add(fea1 + this.sep + fea2);
			else if(fea1.compareTo(fea2) > 0)
				inter_feature.add(fea2 + this.sep + fea1);
		}
		in.close();
	}


	public Sample transform(Sample sample_in) {

		Sample sample_out = (Sample)sample_in.clone();
		
		for(String fea1 : sample_in.getIdFeatures()) {
			for(String fea2 : sample_in.getIdFeatures()) {
				if(fea1.compareTo(fea2) < 0) {
					String new_feature = fea1 + sep + fea2;
					if(inter_feature.contains(new_feature)) 
						sample_out.getIdFeatures().add(new_feature);
				}
			}
		}
		return sample_out;
	}


	public void inplaceTransform(Sample sample_in) {
		return;
	}

	public ArrayList<String> transformFeatures(ArrayList<String> features_in) {
		ArrayList<String> features_out = new ArrayList<String>();
		features_out.addAll(features_in);
		features_out.addAll(inter_feature);
		return features_out;
	}

}
								
