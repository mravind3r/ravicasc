package tdd.copy;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class CascadingCopyUtil {

	public static Flow<?> getFlowConnector(Tap<?, ?, ?> source,
			Tap<?, ?, ?> sink) {
		
		HadoopFlowConnector flowConnector = new HadoopFlowConnector();
		Pipe assembly = new Pipe("copyPipe");
		
		return flowConnector.connect(source,sink,assembly);
	}

	
	
}
