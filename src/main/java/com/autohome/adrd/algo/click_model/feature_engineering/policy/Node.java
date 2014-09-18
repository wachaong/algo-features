package com.autohome.adrd.algo.click_model.feature_engineering.policy;

public class Node {
	private int num_clk;
	private int num_total;
	private Node left_child = null;
	private Node right_child = null;
	private int left_index;
	private int right_index;
	
	public Node() {
		num_clk = 0;
		num_total = 0;
		left_child = null;
		right_child = null;
	}
	
	public double entropy() {
		if(num_clk == 0 || num_clk == num_total)
			return 0.0;
		else {
			double p = ((double)num_clk) / num_total;
			return -(p * Math.log(p) + (1 - p) * Math.log(1 - p));
		}
	}

	public int getNum_clk() {
		return num_clk;
	}

	public void setNum_clk(int num_clk) {
		this.num_clk = num_clk;
	}

	public int getNum_total() {
		return num_total;
	}

	public void setNum_total(int num_total) {
		this.num_total = num_total;
	}

	public Node getLeft_child() {
		return left_child;
	}

	public void setLeft_child(Node left_child) {
		this.left_child = left_child;
	}

	public Node getRight_child() {
		return right_child;
	}

	public void setRight_child(Node right_child) {
		this.right_child = right_child;
	}

	public int getLeft_index() {
		return left_index;
	}

	public void setLeft_index(int left_index) {
		this.left_index = left_index;
	}

	public int getRight_index() {
		return right_index;
	}

	public void setRight_index(int right_index) {
		this.right_index = right_index;
	}
	
}
