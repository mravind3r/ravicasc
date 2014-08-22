package tdd.mapping;

import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SplitterFunction extends BaseOperation implements Function{
	
	

	  public SplitterFunction( Fields fieldDeclaration )
	    {
	    // expects 2 arguments, fail otherwise
	    super( 1, fieldDeclaration );
	    }
	
	
	

	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		TupleEntry tupleEntry = functionCall.getArguments();
		String line = tupleEntry.getString(0);	
		String regex = ",";
		
		Pattern p = Pattern.compile(regex);
		String splits[] = p.split(line);

//		If we initialize the Tuple with the default constructor, 
//		or initialize with the wrong number of objects in the Object[] 
//		then we get the following 
//		exception: cascading.tuple.TupleException: failed to set a value, tuple may not be initialized with values, is zero length.		
		
//		Tuple result = new Tuple(7);
//		result.set(0, splits[0]);
//		result.set(1, splits[1]);
//		result.set(2, splits[2]);
//		result.set(3, splits[3]);
//		result.set(4, splits[4]);
//		result.set(5, splits[5]);
//		result.set(6, splits[6]);
		
		
		
		
		Class<?>[] types =
				 new Class<?>[] {
				 String.class,
				 String.class,
				 String.class,
				 String.class,
				 String.class,
				 String.class,
				 String.class
				 };
		
		
		Tuple result = new Tuple(new Object[types.length]);
		// impossible to get names , this works
		result.set(0, splits[0]);
		result.set(1, splits[1]);
		result.set(2, splits[2]);
		result.set(3, splits[3]);
		result.set(4, splits[4]);
		result.set(5, splits[5]);
		result.set(6, splits[6]);
		
		functionCall.getOutputCollector().add(result);
		
	// another way is to create a tupleentry
		
//		TupleEntry t = new TupleEntry(new Fields(
//                "no","name","sal","dept","country","city","date"),result);
//		
//		t.set("no",splits[0]);
//		t.set("name",splits[1]);
//		t.set("sal",splits[2]);
//		t.set("dept",splits[3]);
//		t.set("country",splits[4]);
//		t.set("city",splits[5]);
//		t.set("date",splits[6]);
//		
//		// now set this to the output collector
//		functionCall.getOutputCollector().add(t);
		
	}

}
