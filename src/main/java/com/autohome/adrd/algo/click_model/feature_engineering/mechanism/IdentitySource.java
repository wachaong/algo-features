package com.autohome.adrd.algo.click_model.feature_engineering.mechanism;

import com.autohome.adrd.algo.click_model.data.Sample;

public class IdentitySource implements Source {

	@Override
	public Sample process(Object raw_data) {
		return (Sample)((Sample) raw_data).clone();
	}
	

}
