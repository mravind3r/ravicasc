package tdd.schema;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class SchemaManipulator {

	public static Flow<?> retainFields(String string, String string2,
			Tap<?, ?, ?> source, Tap<?, ?, ?> sink,RegexSplitter splitter){ 
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		
		Pipe assembly = new Pipe("discardFeilds");
		assembly = new Each(assembly,new Fields("line"),splitter);
		
		Identity identity = new Identity(Fields.ARGS);
		assembly = new Each(assembly,new Fields(string,string2), identity);
		
		FieldJoiner fieldJoiner = new FieldJoiner(",");
		assembly = new Each(assembly, fieldJoiner); 
		
		return flowConnector.connect(source, sink, assembly);
		
		
		
	}

	public static Flow<?> insertNewFields(Tap<?, ?, ?> source,
			Tap<?, ?, ?> sink, String string,RegexSplitter splitter) {
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		Pipe assembly = new Pipe("insertpipe");
		assembly = new Each(assembly,new Fields("line"),splitter);
		
		Fields newField = new Fields("field1");
		Insert insert = new Insert(newField, string);
		
		assembly = new Each(assembly,Fields.ALL,insert,Fields.ALL);
		
		FieldJoiner joiner = new FieldJoiner(",");
		assembly = new Each(assembly,joiner);
		
		return flowConnector.connect(source,sink,assembly);
		
		
	}

}
