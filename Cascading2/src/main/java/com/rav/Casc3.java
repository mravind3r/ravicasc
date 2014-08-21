package com.rav;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Casc3 {

	public static void main(String[] args) {
		System.out.println();
		
		Scheme sourceScheme = new TextLine();
		Tap sourceTap = new Hfs(sourceScheme,"/home/ravi/cascading/file1.txt");
		
		Fields inputTuple = new Fields("exchange","script","date","hi","lo","open","close","vol",
				                       "adj_close");
		String regex = "\\w+\\-?\\w+\\-?\\.?\\w+";
		int groups[] = {1,2,3,4,5,6,7,8,9};
		
        RegexParser regexParser = new
        		RegexParser(inputTuple,regex,groups);
        
        Pipe parserPipe = new Each("parser",new Fields("line"),regexParser);
        
		
		Scheme sinkScheme = new TextLine(new Fields("line"));
		Tap sinkTap = new Hfs(sinkScheme, "/home/ravi/cascading/copy3");
		
		
		
		FlowDef flowDef = FlowDef.flowDef();
		flowDef.addSource(parserPipe, sourceTap);
		flowDef.addTailSink(parserPipe, sinkTap);
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		connector.connect(flowDef).complete();
		
		
		
		
		
		
		
		
		
		
		
	}
	
	
}
