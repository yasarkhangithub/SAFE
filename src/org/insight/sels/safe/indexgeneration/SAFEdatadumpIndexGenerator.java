
package org.insight.sels.safe.indexgeneration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.util.FileManager;

/**
 * Generate Service Description for an RDF data
 * 
 * @author Muhammad Saleem, Yasar Khan
 *
 * 
 */
public class SAFEdatadumpIndexGenerator {

	public static BufferedWriter bw;
	public static long trplCount;

	public static void main(String[] args) throws IOException {
		// --name/location where the services description file will be stored
		bw = new BufferedWriter(new FileWriter(new File("src/ie/deri/hcls/util/SD.n3")));

		// write prfixes to service description
		writePrefixes();
		ArrayList<String> lstSD = new ArrayList<String>();

		// rdf files directory for which services description is required
		File folder = new File("src/ie/deri/hcls/util/rdfdata");
		File[] listOfFiles = folder.listFiles();
		long startTime = System.currentTimeMillis();
		for (File listOfFile : listOfFiles) {
			String rdfFile = "src/ie/deri/hcls/util/rdfdata/" + listOfFile.getName();
			
			// I m stored datasets into local virtuoso sparql endpoint
			String endPointUrl = "http://localhost:8890/sparql";
			String graph = "http://localhost:8890/"
					+ listOfFile.getName().substring(0, listOfFile.getName().length() - 3);
			System.out.println("Generating Summaries: " + listOfFile.getName());
			buildServiceDescription(rdfFile, endPointUrl, graph);
			lstSD.add(listOfFile.getName().substring(2, listOfFile.getName().length()));
		}

		bw.append("#-----------------------------End---------");
		bw.close();
		long endTime = System.currentTimeMillis();
		System.out.println("Data Summaries Generation Time (sec): " + (endTime - startTime) / 1000);
		System.out.print("Data Summaries are secessfully stored at src/org/deri/hcls/util/SD.n3");
	}

	// ----------build service descriptions----------------------------------------------------------------------
	/**
	 * Build Service description for a database
	 * 
	 * @param rdfFile
	 *            RDF datadump for which Service description is required
	 * @param endPointUrl
	 *            Url of the SPARQL endpoint containing datadump rdfFile
	 * @param graph
	 *            RDF Graph name of the data dump
	 */
	public static void buildServiceDescription(String rdfFile, String endPointUrl, String graph) throws IOException {
		Model m = FileManager.get().loadModel(rdfFile);
		ArrayList<String> lstPred = getPredicates(m);
		long totalTrpl = 0;
		bw.append("#---------------------" + endPointUrl + " Descriptions--------------------------------");
		bw.newLine();
		bw.append("[] a sd:Service ;");
		bw.newLine();
		bw.append("     sd:url   <" + endPointUrl + "> ;");
		bw.newLine();
		bw.append("     sd:graph \"" + graph + "\";");
		bw.newLine();

		for (int i = 0; i < lstPred.size(); i++) {
			bw.append("     sd:capability");
			bw.newLine();
			bw.append("         [");
			bw.newLine();
			bw.append("           sd:predicate  <" + lstPred.get(i) + "> ;");
			bw.newLine();
			bw.append("           sd:subjectSelectivity  \"" + getSbjSel(lstPred.get(i), m) + "\" ;");
			bw.newLine();
			bw.append("           sd:objectSelectivity  \"" + getObjSel(lstPred.get(i), m) + "\" ;");
			bw.newLine();
			long trpleCount = getTripleCount(lstPred.get(i), m);
			bw.append("           sd:triples    " + trpleCount + "  ;");
			bw.newLine();
			bw.append("         ] ;");
			bw.newLine();
			totalTrpl = totalTrpl + trpleCount;
		}
		bw.append("     sd:totalTriples \"" + totalTrpl + "\" ;");
		bw.newLine();
		bw.append("             .");
		bw.newLine();

	}

	/**
	 * Get total number of triple for a predicate
	 * 
	 * @param pred
	 *            Predicate
	 * @param m
	 *            model
	 * @return triples
	 */
	public static Long getTripleCount(String pred, Model m) {
		long triples = 0;
		String query = "SELECT  ?s " + //
				"WHERE " + "{" +

		"?s <" + pred + "> ?o " + "} ";
		QueryExecution qexec = QueryExecutionFactory.create(query, m);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			rs.nextSolution();
			triples++;
		}
		return triples;
	}

	// -----------------------------------------------------------
	/**
	 * Get Average Object selectvity for a predicate
	 * 
	 * @param pred
	 *            Predicate
	 * @param m
	 *            Model of the data dump
	 */
	public static double getObjSel(String pred, Model m) {
		double distObj = 0;
		String query = getDistObj(pred);
		QueryExecution qexec = QueryExecutionFactory.create(query, m);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			rs.nextSolution();
			distObj = distObj + 1;
		}

		return (1 / distObj);
	}

	// -------------------------------------------------
	/**
	 * Get Distinct Objects having predicate pred in a RDF file
	 * 
	 * @param pred
	 *            Predicate name
	 */
	private static String getDistObj(String pred) {
		String distObjQuery = "SELECT  DISTINCT ?o " + //
				"WHERE " + "{" +

		"?s <" + pred + "> ?o " + "} ";

		return distObjQuery;
	}

	// -----------------------------------------------------------------------------------------------------------
	/**
	 * Get average subject selectivity of a triple with predicate pred
	 * 
	 * @param pred
	 *            Predicate name
	 */
	public static double getSbjSel(String pred, Model m) {
		double distSbj = 0;
		String query = getDistSbj(pred);
		QueryExecution qexec = QueryExecutionFactory.create(query, m);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			rs.nextSolution();
			distSbj = distSbj + 1;
		}

		// -- avg number of subjects of a specific type (totSbj/distSbj) divided
		// by totSbj => 1/distSbj
		return (1 / distSbj);
	}

	// -------------------------------------------------
	/**
	 * Get Distinct subjects having predicate pred in a RDF file
	 * 
	 * @param pred
	 *            Predicate name
	 */
	private static String getDistSbj(String pred) {
		String distSbjQuery = "SELECT  DISTINCT ?s " + //
				"WHERE " + "{" +

		"?s <" + pred + "> ?o " + "} ";

		return distSbjQuery;
	}

	// ------------Write prefixes used in service
	// description---------------------------------------------
	/**
	 * Write prefixes used in service description
	 */
	private static void writePrefixes() throws IOException {
		/*
		 * bw.append("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.");
		 * bw.newLine(); bw.append(
		 * "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.");
		 * bw.newLine();
		 */
		bw.append("@prefix xsd:  <http://www.w3.org/2001/XMLSchema#>.");
		bw.newLine();
		bw.append("@prefix sd:   <http://darq.sf.net/dose/0.1#>.");
		bw.newLine();
		// bw.append("@prefix darq: <http://darq.sf.net/darq/0.1#>.");
		// bw.newLine();
	}

	// ------------------get predicates
	// lst--------------------------------------------------------------
	/**
	 * Get Predicate List
	 */
	private static ArrayList<String> getPredicates(Model m) {
		ArrayList<String> predLst = new ArrayList<String>();
		String query = getPredQury();
		QueryExecution qexec = QueryExecutionFactory.create(query, m);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			QuerySolution result = rs.nextSolution();
			predLst.add(result.get("p").toString());
			// System.out.println(result.get("p").toString());
		}
		return predLst;
	}

	// --------------------------------------------------------------------------
	/**
	 * Get predicate Query
	 */
	private static String getPredQury() {

		String dataQuery = "SELECT   distinct ?p " + //
				"WHERE " + "{" +

		"?s ?p ?o " + "} ";
		return dataQuery;
	}

}
