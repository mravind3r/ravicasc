package com.regex;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
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

public class CascRegexSplitter1 {

	public static void main(String[] args) {
		
		Scheme ip = new TextLine(new Fields("offset","line"));
		Tap inTap = new Hfs(ip,"/home/ravi/cascading/NYSE_div");

		Scheme op = new TextLine();
		Tap opTap = new Hfs(op,"/home/ravi/cascading/CascRegexSplitter1",SinkMode.REPLACE);
		
		
		RegexSplitter splitter = new RegexSplitter(new Fields("ex",
				                 "script","date","fract"),"\t");
		
		Pipe assembly = new Pipe("nyse");
		assembly = new Each(assembly, new Fields("line"), splitter);
		assembly = new GroupBy(assembly, new Fields("script"));

		//assembly = new Every(assembly, new Fields("fract"),new Sum());
		//Identity identity = new Identity();
		//assembly = new Each(assembly, new Fields("script"),identity);
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		connector.connect(inTap, opTap, assembly).complete();
		
		
		
		
		
		
		
	}
}
