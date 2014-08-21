package tdd.copy;

import org.junit.Test;

import tdd.AssertUtil;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.regex.Prop;

public class TestPlainCopy {

	@Test
	public void test_copy_file(){
		String srcFile = Prop.ipDir + "s1.txt";
		String sinkPath = Prop.opdir+"/copy";
		String destFile = sinkPath +"/part-00000";
		String expectedFile = Prop.ipDir + "s1_res.txt";
		Tap<?, ?, ?> source = new Hfs(new TextLine(new Fields("line")), srcFile);
		Tap<?, ?, ?> sink = new Hfs(new TextLine(), sinkPath,SinkMode.REPLACE);
		Flow flow =  CascadingCopyUtil.getFlowConnector(source,sink);
	    flow.complete();
	    AssertUtil.isContentSame(expectedFile,destFile);
	}
	
}
