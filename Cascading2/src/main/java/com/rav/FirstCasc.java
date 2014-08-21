package com.rav;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class FirstCasc {

	public static void main(String[] args) {
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		
		Tap sourcetap = new Hfs(new TextDelimited(true,"\t"), "/home/ravi/NYSE_daily");
		
		Tap sinkTap = new Hfs(new TextDelimited(true, ","), "/home/ravi/cascading/copy");
		
		Pipe pipe = new Pipe("filecopy");
		
		
		FlowDef flowDef = FlowDef.flowDef();
		flowDef.addSource(pipe, sourcetap);
		flowDef.addTailSink(pipe, sinkTap);
		
		Flow flow = flowConnector.connect(flowDef);
		
		flow.complete();
		
		
		
		
		
	}
	
}	