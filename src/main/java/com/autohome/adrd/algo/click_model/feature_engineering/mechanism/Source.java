package com.autohome.adrd.algo.click_model.feature_engineering.mechanism;

import com.autohome.adrd.algo.click_model.data.Sample;

/**
 * 
 * @brief Transform a data source into a sample.
 *
 */
public interface Source {
	public Sample process(Object raw_data);
}
