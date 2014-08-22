package tdd.mapping;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class CountryFilter extends BaseOperation implements Filter {

	private String country;
	
	public CountryFilter(String countryString){
		this.country = countryString;
	}
	
	public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
		TupleEntry tupleEntry = filterCall.getArguments();
		return tupleEntry.getString("country").equalsIgnoreCase(country);
	}

}
