package tdd.joins;

import java.util.Map;

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

public class TestJoiner {

	//@Test
	public void test_cogroup(){
		String ipFile1 = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/joins/fifth-france-parties.txt";
		String ipFile2 = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/joins/fifth-france-presidents.txt";
		
		String expectedFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/joins/exp";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		Tap<?,?,?> partiesTap = new Hfs(new TextLine(),ipFile1); 
		Tap<?,?,?> presidentTap = new Hfs(new TextLine(),ipFile2); 
		
		Tap<?,?,?> sink = new Hfs(new TextLine(),Prop.opdir+"/copy/",SinkMode.REPLACE);
		RegexSplitter partiesRegex = new RegexSplitter(new Fields("year","party"),"\t");
		RegexSplitter presidentsRegex = new RegexSplitter(new Fields("year","president"),"\t");
		
		Flow<?> flow = Joiner.coGroup(partiesTap,presidentTap,sink,partiesRegex,presidentsRegex);
		flow.writeDOT(Prop.opdir+"/flow1");
		flow.complete();
		
		AssertUtil.isContentSame(expectedFile, actualFile);
	}
	
	
	//@Test
	public void test_coGroup2(){
		String ipFile1 = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/joins/fifth-france-parties.txt";
		String ipFile2 = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/joins/fifth-france-presidents.txt";
		
		String expectedFile = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/joins/exp";
		String actualFile = Prop.opdir+"/copy/part-00000";
		
		Tap<?,?,?> partiesTap = new Hfs(new TextLine(),ipFile1); 
		Tap<?,?,?> presidentTap = new Hfs(new TextLine(),ipFile2); 
		
		Tap<?,?,?> sink = new Hfs(new TextLine(),Prop.opdir+"/copy/",SinkMode.REPLACE);
		RegexSplitter partiesRegex = new RegexSplitter(new Fields("year","party"),"\t");
		RegexSplitter presidentsRegex = new RegexSplitter(new Fields("year","president"),"\t");
		
		Flow<?> flow = Joiner.coGroup2(partiesRegex,presidentTap,partiesRegex,presidentsRegex,sink,partiesTap);
		flow.writeStepsDOT(Prop.opdir+"/flow1");
		flow.complete();
		AssertUtil.isContentSame(expectedFile, actualFile);
	}
	
	
	@Test
	public void create_bins_using_Partitiontap(){
	
		String ipFile1 = "/home/ravi/git/localreposiroty/Cascading2/src/main/java/tdd/joins/fifth-france-parties.txt";
		Tap<?,?,?> source = new Hfs(new TextLine(),ipFile1);
		Tap<?,?,?> sink = new Hfs(new TextLine(),Prop.opdir+"/copy/",SinkMode.REPLACE);
		RegexSplitter partiesRegex = new RegexSplitter(new Fields("year","party"),"\t");
		Flow<?> flow = Joiner.splitOnParties(source,sink,partiesRegex);
		flow.writeDOT(Prop.opdir+"/flow1");
		flow.complete();
		
		
		
	}
	
	
	
}
