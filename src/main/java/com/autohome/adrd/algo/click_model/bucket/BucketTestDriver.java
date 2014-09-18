package com.autohome.adrd.algo.click_model.bucket;

import org.apache.hadoop.fs.Path;

import com.autohome.adrd.algo.click_model.data.SparseVector;
import com.autohome.adrd.algo.click_model.feature_engineering.mechanism.Transformer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.dom4j.io.SAXReader;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.DocumentException;


public class BucketTestDriver {
	
	private String bucket_test_configure_file = null;
	private Document doc = null;
	private ArrayList<String> models = null;
	

	public void setup(String _bucket_test_configure_file) {
		this.bucket_test_configure_file = _bucket_test_configure_file;
		
		SAXReader reader = new SAXReader();
		try {
			doc = reader.read(new File(this.bucket_test_configure_file));
			
		} catch (DocumentException e) {
			e.printStackTrace();
			System.exit(-1);  //configure file not found.
		}
		
		List list = doc.selectNodes("/layers/layer[@type = 'optimization'");

		
    
		for(Iterator layer_iter = list.iterator(); layer_iter.hasNext(); ) { //parse every layer
			String name = null;
			Element experiments = ((Element) layer_iter.next()).element("experiments");
			
			//ArrayList<String> names_layer = new ArrayList<String>();
			for(Iterator iiter = experiments.elementIterator(); iiter.hasNext();) {//parse every experiment
				Element exp = (Element) iiter.next();
				name = exp.attributeValue("name");
				models.add(name);
			}
		}
	}
	
	public void main(String args[]) {
	
		for(String name : models) {
			if(name == "lbfgs")
				return;
			else if(name == "sgd")
				return;
			else if(name == "owlqn")
				return;
			else
				return;
		}
	
	}

}




