package com.autohome.adrd.algo.click_model.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.junit.Before;
import org.junit.Test;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.data.writable.SingleInstanceWritable;
import com.autohome.adrd.algo.click_model.driver.*;

import org.apache.hadoop.io.Text;

public class TestSamplerGeneratorHelper {
	ArrayList<Sample> sample_in = new ArrayList<Sample>();
	private static final String filename = "E:\\data\\train3.csv";
	private static final String configfile = "E:\\data\\ctr2test\\config-loc-2.xml";
	private static final String mapfile = "E:\\data\\ctr2test\\kaggledata\\feature_id_map.txt";
	private Map<String, Integer> feature_id_map = new HashMap<String, Integer>();
	//private KaggleSource ks = new KaggleSource();
	//private Interaction tr = new Interaction();
	private SampleGeneratorHelper helper = new  SampleGeneratorHelper();

	@Before
	public void setUp() throws Exception {
		helper.setup(configfile);
		System.out.println(1);
		feature_id_map = helper.readMaps(mapfile);
		
	}

/*	@Test
	public void testSetup() {
		fail("Not yet implemented");
	}*/

	@Test
	public void testProcess() throws FileNotFoundException {
		Scanner in = new Scanner(new File(filename));
		for(int i = 0; i < 5; ++i) {
			Sample s = helper.process(new Text(in.nextLine()));
			sample_in.add(s);
			System.out.println(s);
			SingleInstanceWritable instance = new SingleInstanceWritable();
			
			for(String fea : s.getIdFeatures()) {
				if(feature_id_map.containsKey(fea))
					instance.addIdFea(feature_id_map.get(fea));
			}
			
			for(Map.Entry<String, Double> ent : s.getFloatFeatures().entrySet()) {
				if(feature_id_map.containsKey(ent.getKey()))
					instance.addFloatFea(feature_id_map.get(ent.getKey()), ent.getValue());
			}

		}
		System.out.println("------------------------");
		
		in.close();

	}

/*	@Test
	public void testGetDatasetFeatures() {
		fail("Not yet implemented");
	}*/

}
