package com.regex;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class CascRegexSplitter {

	public static void main(String[] args) {
		
		Scheme in = new TextLine(new Fields("offset","line"));
		Tap inPutTap = new Hfs(in,"/home/ravi/cascading/emp");
		
		Scheme out = new TextLine(new Fields("dept","sal"));
		Tap outTap = new Hfs(out,"/home/ravi/cascading/CascRegexSplitter",SinkMode.REPLACE);
		
		
		Pipe assembly = new Pipe("CascRegexSplitter");
		Function regexSplitterFunction = new RegexSplitter(
				         new Fields("name","sal","dept"),",");
		
		assembly = new Each(assembly,new Fields("line"),regexSplitterFunction);
		
		assembly = new GroupBy(assembly, new Fields("dept"),true);
		
		Aggregator sum = new Sum();
	
		assembly = new Every(assembly,new Fields("sal"),sum);
		//Aggregator count = new Count(new Fields("count"));
		//assembly = new Every(assembly, new Fields("word"), count);
		
		Identity identity = new Identity(Integer.TYPE);
		assembly = new Each(assembly,new Fields("dept"),identity);
		
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		flowConnector.connect("flow",inPutTap, outTap, assembly).complete();
		
		
		
	}
	
	
}
