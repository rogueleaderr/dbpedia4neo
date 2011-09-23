package org.tinkerpop.rdf;
import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.acaro.dbpedia4neo.inserter.DBpediaLoader;
import org.junit.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.oupls.sail.GraphSail;
import com.tinkerpop.blueprints.pgm.util.TransactionalGraphHelper;
import com.tinkerpop.blueprints.pgm.util.TransactionalGraphHelper.CommitManager;



public class ImportNTTest
{

    @Test
    public void importBerlinAndQuery() throws Exception {
        Neo4jGraph neo = new Neo4jGraph("target/db");
        Sail sail = new GraphSail(neo);
        sail.initialize();
        System.out.println("initialized");
        loadTriples( neo, sail );
        String queryString = "" +
        		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
        		"PREFIX rdfs:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                "PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> " +
                "SELECT ?product ?label"+
                "WHERE { " +
                "?product rdf:type bsbm:Product ."+ //TODO: move this line one down and get no failure
                "?product rdfs:label ?label ."+
                "FILTER regex(?label, 'r')}";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = null ;
        CloseableIteration<? extends BindingSet, QueryEvaluationException> sparqlResults;

        try {
         query = parser.parseQuery(queryString, "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/");
       }
        catch (MalformedQueryException e) {System.out.println ("MalformeSystem.out.printlndQueryException " + e.getMessage()); }
        try {
             sparqlResults = sail.getConnection().evaluate(query.getTupleExpr(), query.getDataset(), new EmptyBindingSet(), false);
         while (sparqlResults.hasNext()) {
             System.out.println("-------------");
             System.out.println("Result: " + sparqlResults.next());
             }
        }
         catch (QueryEvaluationException e) {System.out.println ("QueryEvaluationException " + e.getMessage()); }
         catch (SailException e) {System.out.println ("SailException " + e.getMessage()); }
        sail.shutDown();

    }

    private void loadTriples( Neo4jGraph neo, Sail sail )
            throws RDFParseException, RDFHandlerException,
            FileNotFoundException, IOException, SailException
    {
        CommitManager manager = TransactionalGraphHelper.createCommitManager(neo, 10000);
        File file = new File("berlin_nt_100.nt");
            System.out.println("Loading " + file + ": ");
            DBpediaLoader.loadFile(file.getPath(), sail.getConnection(), sail.getValueFactory(), manager);
            System.out.print('\n');
        manager.close();
    }
}
