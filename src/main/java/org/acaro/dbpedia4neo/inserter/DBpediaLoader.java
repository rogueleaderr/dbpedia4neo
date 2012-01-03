package org.acaro.dbpedia4neo.inserter;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.openrdf.model.ValueFactory;
import org.openrdf.rio.ParseErrorListener;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
//import com.tinkerpop.blueprints.pgm.impls.neo4jbatch.Neo4jBatchGraph;
import com.tinkerpop.blueprints.pgm.oupls.sail.GraphSail;

public class DBpediaLoader 
{
	private static final String LOAD_FROM_DIR = "/Users/rogueleaderr/Data/mapping_dir_a/3";
    private static final String DB_DIR = "/Users/rogueleaderr/Data/var/dbpedia_peter_big_split";
	
	public static void main( String[] args ) 
    	throws SailException, RDFParseException, RDFHandlerException, FileNotFoundException, IOException
    {
		
		File dir = new File(LOAD_FROM_DIR);
    	String[] fileList = dir.list();
		
		
		Neo4jGraph neo = new Neo4jGraph(DB_DIR);
		
    	neo.setMaxBufferSize( 30000 );
    	registerShutdownHook( neo );
    	neo.startTransaction();
    	
    	GraphSail sail = new GraphSail(neo);
    	
    	sail.initialize();
    	
    	//con.setAutoCommit(false);
    	System.out.println("initialized");
    	long start, duration;
    	boolean first = true;
    	
    	if (dir.isDirectory()){
    		for (String child : fileList) {
    			
    			if (".".equals(child) || "..".equals(child) || ".DS_Store".equals(child)) {
    			      continue;  // Ignore the self and parent aliases.
    			    }
    			
    			
    			
    			SailConnection con = sail.getConnection();
    			if (first == false){
    				neo.startTransaction();	
    			}
    			first = false;		
    			
    			start = System.currentTimeMillis();
    			String childFile = (LOAD_FROM_DIR + "/" + child);
    			System.out.println("Uploading: " + child);
    		    		
    			loadFile(childFile, con, sail.getValueFactory());
    			
    			duration = System.currentTimeMillis() - start;
    			System.out.print("Upload took " + duration + '\n');
    			
    			neo.stopTransaction(Conclusion.SUCCESS);
    			con.commit();
    	    	con.close();		
    	    	System.gc();
    	
    		}
    	}
    	
    	sail.shutDown();
    	
    }

	public static void loadFile(final String file, SailConnection sc, ValueFactory vf) throws RDFParseException, RDFHandlerException, FileNotFoundException, IOException {
		NTriplesParser parser = new NTriplesParser(vf);
		TripleHandler handler = new TripleHandler(sc);
		parser.setRDFHandler(handler);
		parser.setStopAtFirstError(false);
		parser.setParseErrorListener(new ParseErrorListener() {
			
			@Override
			public void warning(String msg, int lineNo, int colNo) {
				System.err.println("warning: " + msg);
				System.err.println("file: " + file + " line: " + lineNo + " column: " +colNo);
			}

			@Override
			public void error(String msg, int lineNo, int colNo) {
				System.err.println("error: " + msg);
				System.err.println("file: " + file + " line: " + lineNo + " column: " +colNo);
			}

			@Override
			public void fatalError(String msg, int lineNo, int colNo) {
				System.err.println("fatal: " + msg);
				System.err.println("file: " + file + " line: " + lineNo + " column: " +colNo);
			}
			
		});
		parser.parse(new BufferedInputStream(new FileInputStream(new File(file))), "http://dbpedia.org/");
	}
	
	private static void registerShutdownHook( final Neo4jGraph graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running example before it's completed)
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}
}
