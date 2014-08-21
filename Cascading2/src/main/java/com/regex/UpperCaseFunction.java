package com.regex;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class UpperCaseFunction extends BaseOperation implements Function {

	
	
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		TupleEntry argument = functionCall.getArguments();
		String text = argument.getString(0);
		Tuple result = new Tuple();
		result.add(text.toUpperCase());
		functionCall.getOutputCollector().add(result);
	}

}
