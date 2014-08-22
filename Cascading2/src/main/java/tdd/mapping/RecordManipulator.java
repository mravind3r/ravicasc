package tdd.mapping;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class RecordManipulator {

	public static Flow<?> filterRecords(Tap<?, ?, ?> source, Tap<?, ?, ?> sink,
			RegexSplitter splitter, String string) {
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		Pipe assembly = new Pipe("filterPipe");
		
		assembly = new Each(assembly,new Fields("line"),splitter);
		
		String expression = "country.equals(\"" +string+"\")";
		ExpressionFilter expressionFilter = new ExpressionFilter(expression, String.class);
		
		assembly = new Each(assembly,new Fields("country"),expressionFilter);
		
		FieldJoiner joiner = new FieldJoiner(",");
		assembly = new Each(assembly,Fields.ALL,joiner);
		return flowConnector.connect(source,sink,assembly);
		
	}

	public static Flow<?> filterRecordsUsingCustomFilter(Tap<?, ?, ?> source,
			Tap<?, ?, ?> sink) {
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		Pipe assembly = new Pipe("filterSplitter");
		
		// first i got to write a splitter function to line the fields
		// once you get the right tuple, pass that tuple inside a filter operation
		SplitterFunction splitter = new SplitterFunction();
		assembly = new Each(assembly, new Fields("line"),splitter);
		return connector.connect(source,sink,assembly);
		
	}
	

	
	
//	
//	Pipe A consists of fields, A, B, C, D, and E.  A resulting output stream is to contain data where:
//
//
//		String A = "NONE";
//		Integer B = 10;
//		String C = "VALID";
//		Integer D >= 100; 
//		Long E = 1L;
//
//
//		Cascading Expression Filter
//		 
//		A Cascading Expression Filter involving multiple inputs must have the Field names and data types explicitly defined in Object arrays, resembling the following:
//
//
//		Expression Filter myFilter = 
//		new ExpressionFilter("a.equals("NONE") && b.equals(10) " +
//		                      "&& c.equals("VALID") && (d >= 100) " +
//		                      "&& e.equals(1L), new String[]{"a", "b",
//		                      "c", "d", "e"}, new Class[]{String.class, 
//		                      Integer.class, String.class, Integer.class,
//		                      Long.class});

}
