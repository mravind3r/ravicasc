package patterns.numerical.summarization;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.regex.Prop;

public class Max {
	
  public static void main(String[] args) {
	
	    Scheme lhs = new TextLine(new Fields("line1"));
		Tap lhsTap = new Hfs(lhs,Prop.lhs);
		
		Scheme rhs = new TextLine(new Fields("line2"));
		Tap rhsTap = new Hfs(rhs, Prop.rhs);
		
		
		Scheme opSch = new TextLine();
		Tap sink = new Hfs(opSch,Prop.opdir+"/recordcount",SinkMode.REPLACE);

		

		RegexSplitter splitter = new RegexSplitter(new Fields(
				                      "no","name","sal","dept","country","city","date"),
				                      ",");
		
		Pipe lhsPipe = new Pipe("lhsPipe");
		lhsPipe = new Each(lhsPipe,new Fields("line1"),splitter);
		
		
		//Pipe rhsPipe = new Pipe("rhsPipe");
		//rhsPipe = new Each(rhsPipe,new Fields("line2"),splitter);
		
		
		Pipe assembly = new Pipe("pipe");
		assembly = new GroupBy(lhsPipe,Fields.NONE);
		//assembly = new Every(assembly,new cascading.operation.aggregator.Max());
		
		
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		
		Map<String,Tap> sources = new HashMap();
		sources.put("lhsPipe",lhsTap);
		sources.put("rhsPipe", rhsTap);
		
		connector.connect(lhsTap, sink, assembly).complete();
		
		
	  
  }
}
