package com.rav;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class Casc2 {
	
	public static void main(String[] args) {
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		
		Tap source = new Hfs(new TextDelimited(true, "/t"),"/home/ravi/NYSE_daily");
		Tap sink = new Hfs(new TextDelimited(true, "/t"),"/home/ravi/cascading/copy");
		
		Pipe p = new Pipe("copy pipe");
		
		
		FlowDef flowdef = FlowDef.flowDef();
		flowdef.addSource(p, source);
		flowdef.addTailSink(p, sink);
		
		Flow flow = connector.connect(flowdef);;
		
		flow.complete();
		
		
		
		
	}
	
	
}
