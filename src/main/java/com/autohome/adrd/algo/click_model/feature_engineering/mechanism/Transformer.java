package com.autohome.adrd.algo.click_model.feature_engineering.mechanism;

import java.util.ArrayList;

import com.autohome.adrd.algo.click_model.data.Sample;

/**
 * 将输入的样本 input_sample 做某种变换得到一个新的样本。额外的参数由  file_path 确定的文本文件提供。
 * @author Mingmin Yang
 * 
 *
 */
public interface Transformer {
	public void setup(String conf_path);
	public void inplaceTransform(Sample sample);
	public Sample transform(Sample input_sample);
	public ArrayList<String> transformFeatures(ArrayList<String> features_in);
}
  