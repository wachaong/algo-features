package com.autohome.adrd.algo.click_model.feature_engineering.policy;

import com.autohome.adrd.algo.click_model.feature_engineering.mechanism.OneVarFunction;

public class Log implements OneVarFunction {

	@Override
	public double eval(double x) {
		// TODO Auto-generated method stub
		return Math.log(x);
	}
}
