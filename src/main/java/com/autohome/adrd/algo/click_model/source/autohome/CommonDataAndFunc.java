package com.autohome.adrd.algo.click_model.source.autohome;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Common Data And Function
 * 
 * @author [wangchao]
 */
public class CommonDataAndFunc {

	public static final String CTRL_A = "\u0001";
	public static final String CTRL_B = "\u0002";
	public static final String CTRL_C = "\u0003";
	public static final String CTRL_D = "\u0004";
	public static final String CTRL_E = "\u0005";
	public static final String CTRL_F = "\u0006";
	public static final Long maxKeyWords = 100l;
	public static final String TAB = "\t";
	public static final String SPACE = " ";
	public static final String COLON = ":";
	public static final String PERCENT = "%";
	public static final Long WRITEMAXLENGTH = 1024 * 50l; //
	public static final String DOLLOR = "$";
	public static final String COMMA = ",";
	public static final String AND = "&";
	public static final String AT = "@";

	public static final float NEG_SAMPLE_RATE = 0.01f;
	public static final int SLOT_COUNT = 1000;
	public static final long CLICK_FILTER_THRESHOLD = 1000l;
	public static final long PV_FILTER_THRESHOLD = 1000l;

	public static final String BUCKET_SEP = "->";
	
	/**
	 */
	public static Set<String> readSets(String fileName, String sep, int index,
			String encoding) {
		Set<String> res = new HashSet<String>();
		if (index < 0) {
			return res;
		}

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(fileName), encoding));
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				String[] arr = line.split(sep, -1);
				if (arr.length <= index) {
					continue;
				}
				res.add(arr[index]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return res;
	}

	

	public static Map<String, String> readMapBySep(String fileName,
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
	public static Map<Integer, Double> readMapBySep2(String fileName,
			String encoding, int sepPos) {
		Map<Integer, Double> map = new HashMap<Integer, Double>();
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(fileName), encoding));
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#") || line.trim().isEmpty()) {
					continue;
				}
				String[] tokens = line.split("\t", sepPos);
				//System.out.println(tokens[0]);
				//System.out.println(tokens);
				//System.out.println(sepPos);
				map.put(Integer.parseInt(tokens[0]),Double.parseDouble(tokens[1]));
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
	// @SuppressWarnings("unchecked")
	public static Map<String, String> readMaps(String fileName, String sep,
			int kInd, int vInd, String encoding) {
		Map<String, String> res = new HashMap<String, String>();
		if (kInd < 0 || vInd < 0) {
			return res;
		}
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(fileName), encoding));
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#")) {
					continue;
				}
				String[] arr = line.split(sep, -1);
				if (arr.length <= kInd || arr.length <= vInd) {
					continue;
				}
				res.put((String) arr[kInd], (String) arr[vInd]);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}

}
