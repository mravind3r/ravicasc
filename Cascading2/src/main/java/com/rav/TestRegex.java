package com.rav;

import static org.junit.Assert.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestRegex {

	//@Test
	public void test() {
		String s = "NYSE	CVA	2009-01-02	21.76	22.80	21.46	21.71	1344600	21.71";
		
		String regex = "(\\w+\\-?\\w+\\-?\\.?\\w+)";
		
		Pattern p = Pattern.compile(regex);
		String split[] = p.split(s);
		//System.out.println(split[5]);
		Matcher matcher = p.matcher(s);
		while(matcher.find()){		
			

			System.out.println(matcher.group());
		}
		
	}
	
	
	
	@Test
	public void test2(){
          String s = "doc02	This sinking, dry air produces a rain shadow, "
          		+ "or area in the lee of a mountain with less rain and cloudcover.";
		
		String regex = "([a-zA-Z0-9.,]*)";
		
		Pattern p = Pattern.compile(regex);
		String split[] = p.split(s);
		
		for(int i =0;i<split.length;i++){
		//	System.out.println(split[i]);
		}
		
		
		Matcher matcher = p.matcher(s);
		while(matcher.find()){		
				System.out.println(matcher.group());
		}
	}

}
