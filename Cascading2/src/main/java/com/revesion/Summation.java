package com.revesion;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.regex.Prop;

public class Summation {

	public static void main(String[] args) {
		
		
		Scheme inSch = new TextLine();
		Tap inputTap = new Hfs(inSch,Prop.emp);
		
		Scheme opSch = new TextLine();
		Tap sink = new Hfs(opSch,Prop.opdir+"/recordcount",SinkMode.REPLACE);
		
		
		Pipe summation = new Pipe("recordcounts");
		summation = new GroupBy(summation,Fields.NONE);
		Count counter = new Count();
		summation = new Every(summation, counter);
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		flowConnector.connect(inputTap,sink,summation).complete();
		
		
	}
	
	
}
