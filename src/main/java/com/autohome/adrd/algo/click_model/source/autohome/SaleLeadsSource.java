package com.autohome.adrd.algo.click_model.source.autohome;

import java.util.Scanner;

import org.apache.hadoop.io.Text;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.feature_engineering.mechanism.Source;

public class SaleLeadsSource implements Source {

	@Override
	public Sample process(Object raw_data) {
		Sample sample = new Sample();
		String data = ((Text)raw_data).toString();
		Scanner in = new Scanner(data);
		sample.setLabel(in.nextDouble());
		while(in.hasNext()) {
			String[] tmp = in.next().split(":");
			if(tmp.length < 2) 
				continue;
			if(tmp[0].contains("province") || tmp[0].contains("city"))
				sample.setFeature(tmp[0]);
			else
				sample.setFeature(tmp[0], Double.valueOf(tmp[1]));
		}
		return sample;
	}

}
