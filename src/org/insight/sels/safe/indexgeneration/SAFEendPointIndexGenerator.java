
package org.insight.sels.safe.indexgeneration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

/**
 * Generate SAFE index for a list of Federation members (SPARQL endpoints)
 * 
 * @author Muhammad Saleem, Yasar Khan
 * @see SAFEDatadumpIndexGenerator DatadumpIndexGenerator
 */
public class SAFEendPointIndexGenerator {

	/**
	 * Writes SAFE index into .n3 file
	 */
	public static BufferedWriter bw;

	/**
	 * A Hash map that stores distinct predicates for each endpoint. we will use
	 * this in identifying unique properties and cubes.
	 */
	public static HashMap<String, ArrayList<String>> hmpEndpointPredicates = new HashMap<String, ArrayList<String>>();
	
	
	/**
	 * A Hash map that stores unique (cant find in any other endpoint)
	 * predicates for each endpoint. we will use this in identifying unique
	 * properties and cubes.
	 */
	public static HashMap<String, ArrayList<String>> hmpEndpointUniquePredicates = new HashMap<String, ArrayList<String>>();
	
	
	/**
	 * A hash map that stores a list of cubes per sparql endpoint
	 */
	public static HashMap<String, ArrayList<String>> hmpEndpointCubes = new HashMap<String, ArrayList<String>>();
	
	
	/**
	 * A hash map that stores a list of predicates in a given cube/graph of a
	 * SPARQL endpoint
	 */
	public static HashMap<String, ArrayList<String>> hmpCubePredicates = new HashMap<String, ArrayList<String>>();
	
	
	/**
	 * A hash map that stores a list of unique predicates in a given cube/graph
	 * of a SPARQL endpoint
	 */
	public static HashMap<String, ArrayList<String>> hmpCubeUniquePredicates = new HashMap<String, ArrayList<String>>();
	
	
	/**
	 * use in object part of triple writing in index
	 */
	public static long cubeNo = 1;
	
	
	/**
	 * use in subject part of triple writing in index
	 */
	public static long cubeNoSbj = 1;
	
	
	/**
	 * common properties 
	 */
	public static ArrayList<String> commonProperties = new ArrayList<String>();
	
	

	/**
	 * Returns list of SPARQL endpoints from file i.e. endpointsFile
	 * 
	 * @param endpointsFile
	 *            File containing list of SPARQL endpoints
	 * @return lstEndPoints List of endpoints URLs
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public static ArrayList<String> getEndpointsList(String endpointsFile) throws IOException {
		
		ArrayList<String> lstEndPoints = new ArrayList<String>();
		
		FileInputStream fstream = new FileInputStream(endpointsFile);
		
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		String endpointUrl;
		// Read file records Line By Line
		while ((endpointUrl = br.readLine()) != null) {
			lstEndPoints.add(endpointUrl);
		}
		
		return lstEndPoints;
	}

	
	/**
	 * Provide input information for index generation
	 * 
	 * @param outputFile
	 *            Location of the resulting index file
	 * @throws IOException
	 */
	public static void initializeIndex(String outputFile) throws IOException {
		
		System.out.println("Generating SAFE Index... ");

		// --name/location where the services description file will be stored
		bw = new BufferedWriter(new FileWriter(new File(outputFile)));
		
		// write prfixes to service description
		writePrefixes();
	}
	
	
	
	
	/**
	 * Write prefixes used into SAFE index
	 */
	private static void writePrefixes() throws IOException {
		bw.append("@prefix safe: <http://safe.sels.insight.org/schema/>.");
		bw.newLine();
		//bw.append("@prefix l2s-dim: <http://www.linked2safety-project.eu/dimension/>.");
		//bw.newLine();
		bw.append("@prefix sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>.");
		bw.newLine();
		bw.append("@prefix qb: <http://purl.org/linked-data/cube#> .");
		bw.newLine();
		bw.append("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.");

	}

	
	
	
	
	/**
	 * Build SAFE Index for endpoints
	 * 
	 * @param endPointUrl
	 *            Url of the SPARQL endpoint containing datadump rdfFile
	 * @param graph
	 *            Graph name of the data dump
	 */
	public static void buildIndex(ArrayList<String> lstEndpoints) throws IOException {
		int endpoint;
		loadEndpoint_Predicates_hashMap(lstEndpoints);
		for (endpoint = 0; endpoint < lstEndpoints.size(); endpoint++) {
			String endPointUrl = lstEndpoints.get(endpoint);
			ArrayList<String> lstGraphs = getGraphs(endPointUrl);
			// System.out.println("Total Data Cubes: "+ lstGraphs.size());
			bw.newLine();
			bw.newLine();
			bw.append("#---------------------" + endPointUrl + " --------------------------------");
			bw.newLine();
			bw.append("<http://safe.sels.insight.org/source" + (endpoint + 1) + "> safe:endpointUrl <" + endPointUrl + ">;");
			bw.newLine();
			bw.append("                              safe:cubes");
			writeEndpointCubes(endPointUrl, lstGraphs);
			writeEndpointUniqueProperties(endPointUrl);
			loadCube_UniquePredicates_hashMap(endPointUrl);
			writeEndpointUniqueCubes(endPointUrl);
			writeCubeInformation(lstGraphs);
		}
		
		// System.out.println( hmpEndpointCubes);
		// System.out.println(hmpEndpointPredicates);
		// System.out.println(hmpCubePredicates);
		bw.newLine();
		bw.newLine();
		bw.append("#-------------------------common properties--------------------------");
		bw.newLine();
		bw.append("<http://safe.sels.insight.org/sources/common> safe:commonProperties ");
		writeCommonProperties();
		bw.newLine();
		bw.newLine();
		bw.append("#---------End of SAFE index---------");
		bw.close();
	}
	
	
	

	/**
	 * Load list of unique predicates against individual graphs (data cubes) of a SPARLQ endpoint
	 * 
	 * @param endPointUrl
	 *            SPARQL endpoint URL
	 * @throws IOException
	 */
	private static void loadCube_UniquePredicates_hashMap(String endPointUrl) throws IOException {
		// ArrayList<String> commonProperties=
		// hmpEndpointUniquePredicates.get(endPointUrl);
		ArrayList<String> endpointCubes = hmpEndpointCubes.get(endPointUrl);

		for (int curCube = 0; curCube < endpointCubes.size(); curCube++) {
			Boolean isUpExist = false;

			ArrayList<String> cubeUniqueProperties = new ArrayList<String>();
			// ArrayList<String> endpointProperties=
			// hmpEndpointPredicates.get(endPointUrl);
			// for(int up = 0 ; up<endpointProperties.size();up++)
			// {
			// ArrayList<String> cubePredicates =
			// hmpCubePredicates.get(endpointCubes.get(cube));
			// if (cubePredicates.contains(endpointProperties.get(up)) &&
			// !cubeUniqueProperties.contains(endpointProperties.get(up)))
			// {
			ArrayList<String> curCubeProperties = hmpCubePredicates.get(endpointCubes.get(curCube));
			for (int cp = 0; cp < curCubeProperties.size(); cp++) {
				Boolean test = true;
				for (int othercube = 0; othercube < endpointCubes.size(); othercube++) {
					if (othercube != curCube) {

						ArrayList<String> otherCubePredicates = hmpCubePredicates.get(endpointCubes.get(othercube));
						if (otherCubePredicates.contains(curCubeProperties.get(cp))) {
							test = false;
							break;
						}
					}
				}

				if (test == true && !commonProperties.contains(curCubeProperties.get(cp))) {
					cubeUniqueProperties.add(curCubeProperties.get(cp));
					isUpExist = true;
				}
			}

			if (isUpExist == true)
				hmpCubeUniquePredicates.put(endpointCubes.get(curCube), cubeUniqueProperties);
		}
	}
	
	
	
	

	/**
	 * Write Unique cubes per endpoint into the SAFE index
	 * 
	 * @param endPointUrl
	 *            Endpoint URL
	 * @throws IOException
	 */
	private static void writeEndpointUniqueCubes(String endPointUrl) throws IOException {
		ArrayList<String> endpointCubes = hmpEndpointCubes.get(endPointUrl);
		ArrayList<String> endpointUniqueCubes = new ArrayList<String>();
		// bw.newLine();
		// bw.append(" safe:uniqueCubes safe:CB ");
		for (int cube = 0; cube < endpointCubes.size(); cube++) {
			if (hmpCubeUniquePredicates.containsKey(endpointCubes.get(cube)))
				endpointUniqueCubes.add(endpointCubes.get(cube));
			// bw.append(", safe:"+endpointCubes.get(cube));
			// if(cube == endpointCubes.size()-1)
			// bw.append(".");

		}

		if (!endpointUniqueCubes.isEmpty()) {
			bw.newLine();
			bw.append("							  safe:uniqueCubes");
			for (int uc = 0; uc < endpointUniqueCubes.size(); uc++) {
				if (uc == 0)
					bw.append(" safe:" + endpointUniqueCubes.get(uc) + "");
				else
					bw.append(", safe:" + endpointUniqueCubes.get(uc) + "");

				if (uc == endpointUniqueCubes.size() - 1)
					bw.append(".");

			}

		}

	}
	
	
	
	
	

	/**
	 * Write common properties into SAFE index
	 * 
	 * @throws IOException
	 */
	public static void writeCommonProperties() throws IOException {
		for (int commonProperty = 0; commonProperty < commonProperties.size(); commonProperty++) {
			if (commonProperty == commonProperties.size() - 1)
				bw.append("<" + commonProperties.get(commonProperty) + ">.");
			else
				bw.append("<" + commonProperties.get(commonProperty) + ">, ");
		}
	}
	
	
	
	

	/**
	 * Write the list of unique properties for an endpoint
	 * 
	 * @param endPointUrl
	 *            Endpoint URL
	 * @throws IOException
	 */
	public static void writeEndpointUniqueProperties(String endPointUrl) throws IOException {
		ArrayList<String> endpointProperties = hmpEndpointPredicates.get(endPointUrl);
		ArrayList<String> endpointUniqueProperties = new ArrayList<String>();
		// bw.newLine();
		// bw.append(" safe:uniqueProperties l2s-dim: ");
		for (int property = 0; property < endpointProperties.size(); property++) {
			if (isUniqueProperty(endPointUrl, endpointProperties.get(property)) == true) {
				// bw.append(", <"+endpointProperties.get(property)+">");
				endpointUniqueProperties.add(endpointProperties.get(property));
			}
			// if(property == endpointProperties.size()-1)
			// bw.append(";");

		}
		hmpEndpointUniquePredicates.put(endPointUrl, endpointUniqueProperties);
		if (!endpointUniqueProperties.isEmpty()) {
			bw.newLine();
			bw.append("							  safe:uniqueProperties");
			for (int property = 0; property < endpointUniqueProperties.size(); property++) {
				if (property == 0)
					bw.append(" <" + endpointUniqueProperties.get(property) + ">");
				else
					bw.append(", <" + endpointUniqueProperties.get(property) + ">");

				if (property == endpointUniqueProperties.size() - 1)
					bw.append(";");

			}
		}
	}
	
	
	
	

	/**
	 * Check whether a given property of a endpoint is unique or found somewhere
	 * in other endpoint as well
	 * 
	 * @param endpointUrl
	 *            SPARQL endpoint url
	 * @param property
	 *            Property that needs to be checked
	 * @return value Boolean
	 */
	private static boolean isUniqueProperty(String endpointUrl, String property) {
		boolean value = true;
		Set<String> keySet = hmpEndpointPredicates.keySet();
		Iterator<String> endpointsUrls = keySet.iterator();
		while (endpointsUrls.hasNext() && value == true) {
			String url = endpointsUrls.next();
			if (!url.equals(endpointUrl)) {
				ArrayList<String> endpointProperties = hmpEndpointPredicates.get(url);
				if (endpointProperties.contains(property)) {
					value = false;
					if (!commonProperties.contains(property))
						commonProperties.add(property);
				}
			}

		}

		return value;
	}

	
	
	/**
	 * Write graph name and unique properties per graph/cube
	 * 
	 * @param lstGraphs
	 *            List of Graphs/Cubes
	 * @throws IOException
	 */
	public static void writeCubeInformation(ArrayList<String> lstGraphs) throws IOException {
		for (int graph = 0; graph < lstGraphs.size(); graph++) {
			String cubeName = "CB" + (cubeNoSbj++);
			bw.newLine();
			bw.append("safe:CB" + (cubeNoSbj - 1) + "  safe:graph           <" + lstGraphs.get(graph) + ">.");
			writeCube_uniqueProperties(cubeName);
		}

	}
	
	
	
	/**
	 * 
	 * @param cube
	 * @throws IOException
	 */
	private static void writeCube_uniqueProperties(String cube) throws IOException {

		if (hmpCubeUniquePredicates.containsKey(cube)) {
			ArrayList<String> cubeUpProperties = hmpCubeUniquePredicates.get(cube);
			bw.newLine();
			bw.append("safe:" + cube + "  safe:uniqueProperty ");
			for (int up = 0; up < cubeUpProperties.size(); up++) {
				if (up == 0)
					bw.append(" <" + cubeUpProperties.get(up) + ">");
				else
					bw.append(", <" + cubeUpProperties.get(up) + ">");
			}

			bw.append(". ");
		}
	}

	
	
	
	/**
	 * Write list of Cubes per endpoint into index
	 * 
	 * @param lstGraphs
	 *            List of graphs
	 * @throws IOException
	 */
	public static void writeEndpointCubes(String endpointUrl, ArrayList<String> lstGraphs) throws IOException {
		ArrayList<String> cubes = new ArrayList<String>();
		for (int i = 0; i < lstGraphs.size(); i++) {
			if (i == lstGraphs.size() - 1)
				bw.append(" safe:CB" + (cubeNo++) + ";");
			else
				bw.append(" safe:CB" + (cubeNo++) + ",");
			String cube = "CB" + (cubeNo - 1);
			cubes.add(cube);
			loadCubesToProperties_hashMap(endpointUrl, cube, lstGraphs.get(i));
		}
		hmpEndpointCubes.put(endpointUrl, cubes);
	}

	
	
	/**
	 * 
	 * @param endPoint
	 * @param cubeName
	 * @param cubeUrl
	 */
	public static void loadCubesToProperties_hashMap(String endPoint, String cubeName, String cubeUrl) {
		
		ArrayList<String> predLst = new ArrayList<String>();
		
		String strQuery = getCubePredQury(cubeUrl);
		Query query = QueryFactory.create(strQuery);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
		ResultSet rs = qexec.execSelect();
		
		while (rs.hasNext()) {
			QuerySolution result = rs.nextSolution();
			predLst.add(result.get("p").toString());
			// System.out.println(result.get("p").toString());
		}
		
		hmpCubePredicates.put(cubeName, predLst);
	}
	
	
	
	

	/**
	 * Get SPARQL Query to get unique properties of a specific cube of a
	 * endpoint
	 * 
	 * @param cubeUrl
	 *            Graph or Cube name
	 * @return
	 */
	private static String getCubePredQury(String cubeUrl) {
		String dataQuery = "SELECT   distinct ?p " + " WHERE " + "{" + " Graph <" + cubeUrl + "> {?s ?p ?o} " + "} ";
		// System.out.println(dataQuery);
		return dataQuery;
	}

	
	
	
	
	/**
	 * Get predicate Query for a complete endpoint
	 * 
	 * @return SPARQL Query
	 */
	private static String getPredQury() {

		String dataQuery = "SELECT   distinct ?p " + " WHERE " + "{" + " Graph ?g {?s ?p ?o} " + "} ";
		// System.out.println(dataQuery);
		return dataQuery;
	}

	
	
	/**
	 * Load distinct predicates for each endpoint into a hashmap
	 * 
	 * @param lstEndpoints
	 *            List of SPARQL endpoints
	 */
	public static void loadEndpoint_Predicates_hashMap(ArrayList<String> lstEndpoints) {

		for (int endpoint = 0; endpoint < lstEndpoints.size(); endpoint++)
			hmpEndpointPredicates.put(lstEndpoints.get(endpoint), getPredicates(lstEndpoints.get(endpoint)));

	}
	
	
	

	/**
	 * Get Distinct Predicate List for a given endpoint and specific graph
	 * 
	 * @param endPointUrl
	 *            SPARQL endPoint Url
	 */
	private static ArrayList<String> getPredicates(String endPointUrl) {
		ArrayList<String> predLst = new ArrayList<String>();
		String strQuery = getPredQury();
		Query query = QueryFactory.create(strQuery);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPointUrl, query);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			QuerySolution result = rs.nextSolution();
			predLst.add(result.get("p").toString());
			// System.out.println(result.get("p").toString());
		}
		return predLst;
	}

	
	
	
	
	/**
	 * Get Graph List
	 * 
	 * @param endPointUrl
	 *            SPARQL endPoint Url
	 */
	private static ArrayList<String> getGraphs(String endPointUrl) {
		ArrayList<String> graphLst = new ArrayList<String>();
		String strQuery = getGraphQuery();
		Query query = QueryFactory.create(strQuery);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPointUrl, query);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			QuerySolution result = rs.nextSolution();
			graphLst.add(result.get("g").toString());
			// System.out.println(result.get("p").toString());
		}
		return graphLst;
	}


	
	
	/**
	 * Get Graph Query
	 */
	private static String getGraphQuery() {

		String graphQuery = "SELECT  DISTINCT ?g WHERE  { "
				+ "GRAPH ?g  { ?s ?p ?o. } "
				+ "FILTER (str(?g) != 'http://www.openlinksw.com/schemas/virtrdf#')"
				+ "} ";

		return graphQuery;
	}

}
