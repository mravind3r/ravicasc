package tdd.joins;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class Joiner {


	public static Flow<?> coGroup(Tap<?, ?, ?> partiesTap,
			Tap<?, ?, ?> presidentTap, Tap<?, ?, ?> sink,
			RegexSplitter partiesRegex, RegexSplitter presidentsRegex) {
		
		
		Pipe partiesPipe = new Pipe("parties");
		Pipe presidentsPipe = new Pipe("presidents");
		
		partiesPipe = new Each(partiesPipe, new Fields("line"),partiesRegex);
		presidentsPipe = new Each(presidentsPipe, new Fields("line"),presidentsRegex);
		
		Fields common = new Fields("year");
		Fields groupFields = new Fields("year1","party","year2","president");
		
		Pipe assembly = new CoGroup(partiesPipe,common,presidentsPipe,common,groupFields,new InnerJoin());
		
		
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		
		
		Map<String,Tap> sources = new HashMap<String, Tap>();
		sources.put("parties",partiesTap);
		sources.put("presidents", presidentTap);
		
		return flowConnector.connect(sources, sink, assembly);
		
	}

	public static Flow<?> coGroup2(RegexSplitter partiesRegex,
			Tap<?, ?, ?> presidentTap, RegexSplitter partiesRegex2,
			RegexSplitter presidentsRegex, Tap<?, ?, ?> sink, Tap<?,?,?> partiesTap) {
		
		Pipe partiesPipe = new Pipe("parties");
		partiesPipe = new Each(partiesPipe, new Fields("line"),partiesRegex);
		
		Pipe presidentsPipe = new Pipe("presidents");
		presidentsPipe = new Each(presidentsPipe, new Fields("line"),presidentsRegex);
		
		

		Fields key = new Fields("year");
		Fields outputFields = new Fields("year1","party","year2","president");
		Pipe assembly = new CoGroup(partiesPipe,key,presidentsPipe,key,outputFields,new InnerJoin());
		
		assembly = new Each(assembly,new Fields("year1","party","president"),new Identity());
		
		Map<String,Tap> sources = new HashMap<String, Tap>();
		sources.put("parties", partiesTap);
		sources.put("presidents", presidentTap);
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		return flowConnector.connect(sources, sink, assembly);
		
	}

	public static Flow<?> splitOnParties(Tap<?, ?, ?> source,
			Tap<?, ?, ?> sink, RegexSplitter partiesRegex) {
		
	
		
		
	return null;	
		
	}

}
