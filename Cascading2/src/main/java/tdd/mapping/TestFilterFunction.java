package tdd.mapping;

import org.junit.Test;

import tdd.AssertUtil;
import cascading.flow.Flow;
import cascading.operation.regex.RegexSplitter;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.regex.Prop;


public class TestFilterFunction {

	// filter every employee record from country = columbia
	//@Test
	public void test_country_filter(){
		
		String ipFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/mapping/source";
		String expectedFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/mapping/exp1";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		
		Tap<?,?,?> source = new Hfs(new TextLine(),ipFile);
		
		RegexSplitter splitter = new RegexSplitter(new Fields(
                "no","name","sal","dept","country","city","date"),
                ",");
       
		Tap<?,?,?> sink = new Hfs(new TextLine(), Prop.opdir+"/copy",SinkMode.REPLACE);
		Flow<?> flow = RecordManipulator.filterRecords(source,sink,splitter,"Colombia");
		flow.writeDOT(Prop.opdir+"/flow1");
		flow.complete();
		AssertUtil.isContentSame(expectedFile, actualFile);
		
	}
	
	
	@Test
	public void test_country_filter_using_custom_filter_and_custom_function(){
		
		
		String ipFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/mapping/source";
		String expectedFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/mapping/exp1";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		Tap<?,?,?> source = new Hfs(new TextLine(new Fields("line")), ipFile);
		Tap<?,?,?> sink = new Hfs(new TextLine(),Prop.opdir+"/copy",SinkMode.REPLACE);
		
		//pass data via splitter -function operation, filter operation
		// here we shall split inside the custom function hence not passing regex
		Flow<?> flow = RecordManipulator.filterRecordsUsingCustomFilter(source,sink,"colombia");
		flow.writeDOT(Prop.opdir+"/flow1");
		flow.complete();
		
		AssertUtil.isContentSame(expectedFile, actualFile);
		
		
	}
	
	
	
}
