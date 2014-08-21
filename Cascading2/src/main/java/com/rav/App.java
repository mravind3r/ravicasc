package com.rav;


import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Properties properties = new Properties();
    	 
    	AppProps.setApplicationJarClass( properties, App.class );
    	HadoopFlowConnector flowConnector = new HadoopFlowConnector();
     
    	// create the source tap
    	Tap inTap = new Hfs( new TextDelimited( true, "\t" ), "/home/ravi/NYSE_daily" );
        
    	// create the sink tap
    	Tap outTap = new Hfs( new TextDelimited( true, "\t" ), "/home/ravi/out.txt" );
     
    	// specify a pipe to connect the taps
    	Pipe copyPipe = new Pipe( "copy" );
     
    	// connect the taps, pipes, etc., into a flow
    	FlowDef flowDef = FlowDef.flowDef()
    	    .addSource( copyPipe, inTap )
    	    .addTailSink( copyPipe, outTap );
     
    	// run the flow
    	Flow flow = flowConnector.connect( flowDef );
    	flow.complete();    }
}
