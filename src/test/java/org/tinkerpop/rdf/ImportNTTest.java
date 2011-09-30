package org.tinkerpop.rdf;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.oupls.sail.GraphSail;

public class ImportNTTest
{

    @Test
    public void importBerlinAndQuery() throws Exception
    {
        Neo4jGraph neo = new Neo4jGraph( "target/db" );
        neo.setMaxBufferSize( 5000 );
        Sail sail = new GraphSail( neo );
        sail.initialize();
        SailRepositoryConnection connection = new SailRepository(sail).getConnection();
        System.out.println( "initialized" );
//        loadTriples( neo, connection );
        String queryString1 = "" +
        		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        		"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
        		"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
        		"SELECT ?product ?label " +
        		"WHERE {" +
        		"    ?product rdfs:label ?label ." +
        		"    ?product rdf:type bsbm:Product ." +
        		"    FILTER regex(?label, \"r\")}";
        String queryString2 = "" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                "PREFIX rdfs:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
                "SELECT ?product " +
                "WHERE { ?product rdf:type bsbm:Product .} ";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = null;
        CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;

        try
        {
            query = parser.parseQuery( queryString1,
                    "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/" );
        }
        catch ( MalformedQueryException e )
        {
            System.out.println( "MalformeSystem.out.printlndQueryException "
                                + e.getMessage() );
        }
        try
        {
            sparqlResults = sail.getConnection().evaluate(
                    query.getTupleExpr(), query.getDataset(),
                    new EmptyBindingSet(), false );
            while ( sparqlResults.hasNext() )
            {
                System.out.println( "-------------" );
                System.out.println( "Result: " + sparqlResults.next() );
            }
        }
        catch ( QueryEvaluationException e )
        {
            System.out.println( "QueryEvaluationException " + e.getMessage() );
        }
        catch ( SailException e )
        {
            System.out.println( "SailException " + e.getMessage() );
        }
        sail.shutDown();

    }

    private void loadTriples( Neo4jGraph neo, SailRepositoryConnection connection )
            throws RDFParseException, RDFHandlerException,
            FileNotFoundException, IOException, SailException
    {
        File file = new File( "berlin_nt_100.nt" );
        System.out.println( "Loading " + file + ": " );
        try
        {
            connection.add(file, null, RDFFormat.NTRIPLES);
        }
        catch ( RepositoryException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.print( '\n' );
    }
}
