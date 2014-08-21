package com.rav;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexParser;
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

public class WordCount2 {

	public static void main(String[] args) {
		
		Scheme inputScheme = new TextLine();
		Tap sourceTap = new Hfs(inputScheme, "/home/ravi/cascading/file1.txt");
		
		Scheme outScheme = new TextLine();
		Tap sinkTap = new Hfs(outScheme,"/home/ravi/cascading/copy3", SinkMode.REPLACE);
		
	
		String regex = "\\w+\\-?\\w+\\-?\\.?\\w+";
		Function logParserFunction = new RegexGenerator(new Fields("word"),regex);
		
		Pipe assemblyPipe = new Pipe("word-count");
		assemblyPipe = new Each(assemblyPipe,new Fields("line"),logParserFunction);
		assemblyPipe = new GroupBy(assemblyPipe,new Fields("word"));
		
		Aggregator count = new Count(new Fields("count"));
		assemblyPipe = new Every(assemblyPipe, count);
		
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		Flow flow = flowConnector.connect( "word-count", sourceTap, sinkTap, assemblyPipe );
		flow.complete();
		
		
		
	}
	
}
