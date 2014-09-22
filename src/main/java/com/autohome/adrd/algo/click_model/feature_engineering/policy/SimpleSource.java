package com.autohome.adrd.algo.click_model.feature_engineering.policy;

import com.autohome.adrd.algo.click_model.data.Sample;
import com.autohome.adrd.algo.click_model.feature_engineering.mechanism.Source;



public class SimpleSource implements Source {

	@Override
	public Sample process(Object raw_data) {
		return (Sample) raw_data;
	}

}
