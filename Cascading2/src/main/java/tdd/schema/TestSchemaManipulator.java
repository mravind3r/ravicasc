package tdd.schema;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.operation.regex.RegexSplitter;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.regex.Prop;

import tdd.AssertUtil;

public class TestSchemaManipulator {

	//@Test
	public void test_discard_fields_from_file(){
		String ipFile = "/home/ravi/workspace/Cascading2/src/main/java/tdd/schema/source";
		String expectedFile = "/home/ravi/workspace/Cascading2/src/main/java/tdd/schema/exp1";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		Tap<?, ?, ?> source = new Hfs(new TextLine(new Fields("line")), ipFile);
		
		RegexSplitter splitter = new RegexSplitter(new Fields(
                "no","name","sal","dept","country","city","date"),
                ",");
       
		Tap<?,?,?> sink = new Hfs(new TextLine(), Prop.opdir+"/copy",SinkMode.REPLACE);
	   Flow<?> flow = SchemaManipulator.retainFields("no","name",source,sink,splitter);
       flow.complete();
		
		AssertUtil.isContentSame(expectedFile,actualFile);
	}
	
	
	@Test
	public void test_insert_new_field_into_file(){
		String ipFile = "/home/ravi/workspace/Cascading2/src/main/java/tdd/schema/source";
		String expectedFile = "/home/ravi/workspace/Cascading2/src/main/java/tdd/schema/exp2";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		Tap<?, ?, ?> source = new Hfs(new TextLine(new Fields("line")), ipFile);
		
		RegexSplitter splitter = new RegexSplitter(new Fields(
                "no","name","sal","dept","country","city","date"),
                ",");
       
		Tap<?,?,?> sink = new Hfs(new TextLine(), Prop.opdir+"/copy",SinkMode.REPLACE);
		
		
		Flow<?> flow = SchemaManipulator.insertNewFields(source,sink,"1",splitter);
		
		flow.complete();
		
		AssertUtil.isContentSame(expectedFile, actualFile);
	
	}
	
	
}
