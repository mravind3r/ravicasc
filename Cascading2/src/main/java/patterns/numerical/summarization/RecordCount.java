package patterns.numerical.summarization;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.regex.Prop;


// count the number of records and output just the value  eg: 200 


public class RecordCount {
	
	public static void main(String[] args) {
	
		Scheme inSch = new TextLine();
		Tap inputTap = new Hfs(inSch,Prop.emp);
		
		Scheme opSch = new TextLine();
		Tap sink = new Hfs(opSch,Prop.opdir+"/recordcount",SinkMode.REPLACE);
		
		
		Pipe assembly = new Pipe("recordCount");
		assembly = new GroupBy(assembly,Fields.NONE);
	
		Count count = new Count(new Fields("counter"));
		assembly = new Every(assembly,count);
		
		
		
		
		
		
		
		
		
		
		HadoopFlowConnector connector = new HadoopFlowConnector();
		connector.connect(inputTap,sink,assembly).complete();
		
		
		
		
		
		
		
		
		
		
	}
	

}
