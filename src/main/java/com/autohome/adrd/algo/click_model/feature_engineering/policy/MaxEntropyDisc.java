package com.autohome.adrd.algo.click_model.feature_engineering.policy;

import java.util.Vector;

public class MaxEntropyDisc {
	private Node root = null;
	private Vector<Integer> value = new Vector<Integer>();
	private Vector<Integer> click = new Vector<Integer>();
	private Vector<Integer> total = new Vector<Integer>();
	
	public MaxEntropyDisc(Vector<Integer> value, Vector<Integer> click, Vector<Integer> total) {
		this.value = value;
		this.click = click;
		this.total = total;
	}
	
	public double split(Node node) {
		boolean splited = false;
		double entropy = node.entropy();
		double entropy_new = entropy;
		double entropy_min = entropy;
		double max_gain = 0.0;
		
		Node left = new Node();
		Node right = new Node();
		Node left_child = null;
		Node right_child = null;
		int splitPoint = 0;
		
		
		for(int i = node.getLeft_index(); i != node.getRight_index(); ++i) {
			left.setNum_clk(left.getNum_clk() + click.get(i));
			right.setNum_clk(right.getNum_clk() - click.get(i));
			left.setNum_total(left.getNum_total() + total.get(i));
			right.setNum_total(right.getNum_total() - total.get(i));
			double p = ((double)left.getNum_total()) / node.getNum_total();
			entropy_new = p * left.entropy() + (1 - p) * right.entropy();
			if(entropy_new < entropy_min) {
				entropy_min = entropy_new;
				splitPoint = i;
				splited = true;
				left_child = left;
				right_child = right;
				max_gain = entropy - entropy_min;
				
			}
		}
		if(splited) {
			node.setLeft_child(left_child);
			node.setRight_child(right_child);
		}
		
		return max_gain;	
	}

}
