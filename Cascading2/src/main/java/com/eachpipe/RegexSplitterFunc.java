package com.eachpipe;

import com.regex.Prop;

import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class RegexSplitterFunc {

	public static void main(String[] args) {
		Tap<?, ?, ?> sourceTap = new Hfs(new TextLine(new Fields("line")),Prop.emp);
		
		
		
	}
	
	
}
