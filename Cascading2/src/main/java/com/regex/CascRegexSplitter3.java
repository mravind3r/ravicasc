package com.regex;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class CascRegexSplitter3 {

	public static void main(String[] args) {
		
		Scheme in = new TextLine();
		Tap inputTap = new Hfs(in,Prop.nyse_div);
		
		Scheme out = new TextLine();
		Tap sink = new Hfs(out,Prop.opdir+"/split4",SinkMode.REPLACE);
		
		RegexSplitter regexSplitter = new RegexSplitter(new Fields("ex","script","date","fract"), "\t");
		Pipe assembly = new Pipe("pipe");
		
		assembly = new Each(assembly, new Fields("line"), regexSplitter,Fields.RESULTS);
		
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		connector.connect(inputTap, sink, assembly).complete();
		
		
		
		
		
		
		
		
		
		
		
	}
	
}
