package com.regex;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class CascRegexSplitter2 {

	public static void main(String[] args) {
		
		Scheme ip = new TextLine(new Fields("byteoffset","line"));
		Tap ipTap = new Hfs(ip, Prop.nyse_div);
		
		Scheme op = new TextLine();
		Tap sink = new Hfs(op,Prop.opdir+"/split2",SinkMode.REPLACE);
		
		
		Pipe assembly = new Pipe("filter");
		
		RegexSplitter regexSplitterFunction = 
				new RegexSplitter(new Fields("ex","script","date","fract"),"\t");
		
		assembly = new Each(assembly, new Fields("line"),regexSplitterFunction,new Fields("script","date"));
		
		assembly = new GroupBy(assembly,new Fields("date"));
		
	//	assembly = new Each(assembly,new Fields("script","date","fract"),new Identity()) ;
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		connector.connect(ipTap, sink, assembly).complete();;
		
	}
	
	
}
