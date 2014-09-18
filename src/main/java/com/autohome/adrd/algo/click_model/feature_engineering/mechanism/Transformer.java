package com.autohome.adrd.algo.click_model.feature_engineering.mechanism;

import java.util.ArrayList;

import com.autohome.adrd.algo.click_model.data.Sample;

/**
 * ����������� input_sample ��ĳ�ֱ任�õ�һ���µ�����������Ĳ�����  file_path ȷ�����ı��ļ��ṩ��
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
  