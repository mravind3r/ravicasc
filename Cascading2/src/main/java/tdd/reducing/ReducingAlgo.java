package tdd.reducing;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Debug;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class ReducingAlgo {

	public static Flow<?> countWords(Tap<?, ?, ?> source, Tap<?, ?, ?> sink,
			Function regexSplitter) {
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		Pipe assembly = new Pipe("wordcount");
		assembly = new Each(assembly, new Fields("line"),regexSplitter);
		assembly = new GroupBy(assembly,new Fields("word"));
		//assembly = new Each(assembly, new Debug());
		assembly = new Every(assembly,new Count());
		assembly = new Each(assembly, new Debug());
		return flowConnector.connect(source,sink,assembly);
		
	}

	public static Flow<?> wordCountEfficiently(Tap<?, ?, ?> source,
			Tap<?, ?, ?> sink, RegexGenerator generator) {
		
		Pipe assembly = new Pipe("wordcount");
		assembly = new Each(assembly,new Fields("line"),generator);
		assembly = new Each(assembly,new Debug());
		assembly = new CountBy(assembly, new Fields("word"),new Fields("count"));
		HadoopFlowConnector connector = new HadoopFlowConnector();
		return connector.connect(source,sink,assembly);
		
		
		
		
	}

	
	
	
}
