package com.regex;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class CascRegexSplitterWc {

	public static void main(String[] args) {
		
		Scheme ip = new TextLine();
		Tap inTap = new Hfs(ip,Prop.rain);
		
		Scheme op = new TextLine();
		Tap optap = new Hfs(op,Prop.opdir+"/rain-res",SinkMode.REPLACE);
		
		
		Function regexSplitterFunc = new RegexSplitter(new Fields("token","text"),"\t");
		Pipe assembly = new Pipe("wc");
		
		assembly = new Each(assembly,new Fields("line"),regexSplitterFunc,Fields.RESULTS);
		assembly = new Retain(assembly, Fields.LAST);
		
//		Function regexSplitterFunc2 = new RegexGenerator(new Fields("word"),"([\\w,.]*)");
//		assembly = new Each(assembly,new Fields("text"),regexSplitterFunc2,Fields.RESULTS);
//		assembly = new GroupBy(assembly,new Fields("word"));
//		assembly = new Every(assembly,new Fields("word"),new Count());
		Function regexSplitterFunc2 = new RegexGenerator(new Fields("word"),"([\\w,.]*)");
		
		assembly = new Each(assembly, new Fields("text"),new UpperCaseFunction(),Fields.RESULTS);
		
		//assembly = new GroupBy(assembly,new Fields("word"));
		
		//assembly = new Every(assembly,new Fields("word"),new Count());
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		connector.connect(inTap,optap,assembly).complete();
		
		
		
		
		
		
		
	}
	
	
}
