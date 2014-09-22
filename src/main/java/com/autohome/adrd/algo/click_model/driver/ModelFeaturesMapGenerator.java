package com.autohome.adrd.algo.click_model.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * 
 * @author Yang Mingmin
 *
 */
public class ModelFeaturesMapGenerator {
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		/*String args[] = new String[5];
		args[0] = "E:\\data\\ctr2test\\config-loc-2.xml";
		args[1] = "E:\\data\\ctr2test\\Orignsinglecategoryfeaturefequencyfilter100Trans";
		args[2] = "E:\\data\\ctr2test\\feature_id_map.txt";
		args[3] = "E:\\data\\ctr2test\\model_featuresId_map.txt";
		args[4] = "E:\\data\\ctr2test\\init_weight.txt";*/
		if(args.length < 3) {
			System.out.println("Useage: [config-file] [input-features] [output-file]");
			System.exit(-1);
		}
		ArrayList<String> input_features = new ArrayList<String>();
		SampleGeneratorHelper helper = new SampleGeneratorHelper();
		helper.setup(args[0]);
		input_features = readArrayList(args[1]);
		Map<String, ArrayList<String>> model_feature_map = helper.getDatasetFeatures(input_features);
		
		//output the results
		PrintWriter out = new PrintWriter(new OutputStreamWriter(  
		                new FileOutputStream(args[2]),  
		                "UTF-8"));  

		for(Map.Entry<String, ArrayList<String>> entry : model_feature_map.entrySet()) {
			out.write(entry.getKey());
			out.write("\t");
			int n = 0;
			for(String fea : entry.getValue()) {
				if(n != 0)
					out.write(",");
				out.write(fea);
				n++;
			}	
		}
		out.close();
	}
	
	private static ArrayList<String> readArrayList(String path) throws FileNotFoundException {
		ArrayList<String> result = new ArrayList<String>();
		Scanner fin = new Scanner(new File(path));
		while(fin.hasNext()) {
			result.add(fin.next());
		}
		return result;
	}

}
