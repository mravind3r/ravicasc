package com.rav;

import java.util.Properties;






import cascading.flow.Flow;
import cascading.flow.FlowConnector;
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

public class WordCount {

	
	public static void main(String[] args) {
		System.out.println();
		Scheme sourceScheme = new TextLine( new Fields( "line1" ) );
		Tap source = new Hfs( sourceScheme, "/home/ravi/cascading/file1.txt" );

		Scheme sinkScheme = new TextLine( new Fields( "word", "count" ) );
		Tap sink = new Hfs( sinkScheme, "/home/ravi/cascading/copy3", SinkMode.REPLACE );

		// the 'head' of the pipe assembly
		Pipe assembly = new Pipe( "wordcount" );

		// For each input Tuple
		// parse out each word into a new Tuple with the field name "word"
		// regular expressions are optional in Cascading
	//	String regex = "\\s";
		String regex = "\\w+\\-?\\w+\\-?\\.?\\w+";
		Function function = new RegexGenerator( new Fields( "word" ), regex );
//		String regex = "\\w+\\-?\\w+\\-?\\.?\\w+";
//		Function logParserFunction = new RegexGenerator(new Fields("word"),regex);

		
		
		assembly = new Each( assembly, new Fields( "line1" ), function );
		// group the Tuple stream by the "word" value
		assembly = new GroupBy( assembly, new Fields( "word" ) );
		
//		Pipe assemblyPipe = new Pipe("word count");
//		assemblyPipe = new Each(assemblyPipe,new Fields("line"),logParserFunction);
//		assemblyPipe = new GroupBy(assemblyPipe,new Fields("word"));
		
		
		// For every Tuple group
		// count the number of occurrences of "word" and store result in
		// a field named "count"
		Aggregator count = new Count( new Fields( "count" ) );
		assembly = new Every( assembly, count );

//		Aggregator count = new Count(new Fields("word")); --- error
//		assemblyPipe = new Every(assemblyPipe, count);
		
		
		
		
		// plan a new Flow from the assembly using the<Context> source and sink Taps
		// with the above properties
		HadoopFlowConnector connector = new HadoopFlowConnector();
		Flow flow = connector.connect( "word-count", source, sink, assembly );

		// execute the flow, block until complete
		flow.complete();
		
		
		
		
		
	}
}
