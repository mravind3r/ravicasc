package com.revesion;

import com.regex.Prop;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Max;
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

public class SumSalsByDept {

	public static void main(String[] args) {
		

		Scheme inSch = new TextLine();
		Tap inputTap = new Hfs(inSch,Prop.emp);
		
		Scheme opSch = new TextLine();
		Tap sink = new Hfs(opSch,Prop.opdir+"/recordcount",SinkMode.REPLACE);


		RegexSplitter splitter = new RegexSplitter(new Fields(
				                      "no","name","sal","dept","country","city","date"),
				                      ",");
		
		Pipe assembly = new Pipe("sumsalsbydept");
		assembly = new Each(assembly,new Fields("line"),splitter);
		
		assembly = new GroupBy(assembly,new Fields("dept"));
		
		//assembly = new Every(assembly,new Fields("sal"),new Sum());
		assembly = new Every(assembly,new Fields("sal"),new Count(),Fields.ALL);
		assembly = new Every(assembly, new Fields("sal"),new Sum());
		assembly = new Every(assembly,new Fields("sal"),new Average());
		assembly = new Every(assembly,new Fields("sal"),new Max());
		
		
		
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		connector.connect(inputTap,sink,assembly).complete();
		
		
		
		
		
	}
	
}
