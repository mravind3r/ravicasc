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

}
