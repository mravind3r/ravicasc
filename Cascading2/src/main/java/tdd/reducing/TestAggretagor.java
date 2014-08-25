package tdd.reducing;

import org.junit.Test;

import tdd.AssertUtil;
import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.regex.Prop;

public class TestAggretagor {

	//@Test
	public void test_built_in_aggregator_sum_operation(){
		
		String ipFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/reducing/source";
		String expectedFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/reducing/exp";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		Tap<?,?,?> source = new Hfs(new TextLine(), ipFile);
		Tap<?,?,?> sink = new Hfs(new TextLine(),Prop.opdir+"/copy/",SinkMode.REPLACE);
		
		String regex = "\\w+\\-?\\w+\\-?\\.?\\w+";
		Function logParserFunction = new RegexGenerator(new Fields("word"),regex);
		
		
		Flow<?> flow =  ReducingAlgo.countWords(source,sink,logParserFunction);
		flow.writeDOT(Prop.opdir+"/flow1");
		flow.complete();
		AssertUtil.isContentSame(expectedFile, actualFile);
		
		
		
	}
	
	@Test
	public void test_efficeint_aggregator(){
		
		String ipFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/reducing/source";
		String expectedFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/reducing/exp";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		Tap<?,?,?> source = new Hfs(new TextLine(), ipFile);
		Tap<?,?,?> sink = new Hfs(new TextLine(),Prop.opdir+"/copy/",SinkMode.REPLACE );
		String patternString = "\\w+\\-?\\w+\\-?\\.?\\w+";
		RegexGenerator generator = new RegexGenerator(new Fields("word"),patternString);
		
		Flow<?> flow = ReducingAlgo.wordCountEfficiently(source,sink,generator);
		flow.writeDOT(Prop.opdir+"/flow1");
		flow.complete();
		AssertUtil.isContentSame(expectedFile, actualFile);
		
		
	}
	
	
}
