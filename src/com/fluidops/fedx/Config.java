/*
 * Copyright (C) 2008-2013, fluid Operations AG
 *
 * FedX is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.fluidops.fedx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.insight.sels.safe.main.QueryEvaluation;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.memory.MemoryStore;

import com.fluidops.fedx.cache.MemoryCache;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.SailFederationEvalStrategy;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategy;
import com.fluidops.fedx.evaluation.SparqlFederationEvalStrategyWithValues;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.exception.FedXRuntimeException;
import com.fluidops.fedx.monitoring.QueryLog;
import com.fluidops.fedx.monitoring.QueryPlanLog;
import com.fluidops.fedx.provider.ProviderUtil;

/**
 * Configuration properties for FedX based on a properties file. Prior to using
 * this configuration {@link #initialize(String)} must be invoked with the
 * location of the properties file.
 * 
 * @author Andreas Schwarte
 *
 */
public class Config {

	protected static Logger log = Logger.getLogger(Config.class);

	private static Config instance = null;

	////////////////////////////////////////////////////////////////////////////
	/////////// Added Content for SAFE ////////////////////////////////////////

	public static RepositoryConnection con = null;
	public static Set<String> lstC = new HashSet<String>(); // common properties
															// in all of the
															// sources
	public static ArrayList<String> lstS1 = new ArrayList<String>(); // Source 1
																		// unique
																		// properties
	public static ArrayList<String> lstS2 = new ArrayList<String>(); // source 2
																		// unique
																		// properties
	public static ArrayList<String> lstS3 = new ArrayList<String>(); // source 3
																		// unique
																		// properties
	public static HashMap<String, ArrayList<String>> uniqueCubes = new HashMap<String, ArrayList<String>>(); // group
																												// of
																												// cubes
																												// with
																												// unique
																												// properties
																												// in
																												// each
																												// source
	public static HashMap<String, ArrayList<String>> graphs = new HashMap<String, ArrayList<String>>(); // list
																										// of
																										// graphs
																										// per
																										// source
	public static HashMap<String, String> uniqueProperty = new HashMap<String, String>(); // cube
																							// ->
																							// its
																							// unique
																							// property
																							// hash
																							// map
	
	
	
	public static HashMap<String, List<String>> cubeUniquePropertiesMap = new HashMap<String, List<String>>(); // cube to its unique properties list
	
	
	public static HashMap<String, String> graph = new HashMap<String, String>(); // cube
																					// ->
																					// its
																					// graph
																					// name
																					// hash
																					// map
	public static HashMap<String, Set<String>> uniqueProperties = new HashMap<String, Set<String>>(); // source
																										// ->
																										// its
																										// unique
																										// properties
																										// hash
																										// map
	public static ArrayList<String> endPoints = new ArrayList<String>(); // complete
																			// list
																			// of
																			// endpoints
	public static Set<String> allUniqueProperties = new HashSet<String>();
	private static String filePath = QueryEvaluation.indexPath;

	///////////////////////////////////////////////////////////////////////////

	public static Config getConfig() {
		if (instance == null)
			throw new FedXRuntimeException("Config not initialized. Call Config.initialize() first.");
		return instance;
	}

	protected static void reset() {
		instance = null;
	}

	/**
	 * Initialize the configuration with the specified properties file.
	 * 
	 * @param fedxConfig
	 *            the optional location of the properties file. If not specified
	 *            the default configuration is used.
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	public static void initialize(String... fedxConfig) throws FedXException, RepositoryException,
			MalformedQueryException, QueryEvaluationException, RDFParseException, IOException {
		if (instance != null)
			throw new FedXRuntimeException("Config is already initialized.");
		instance = new Config();
		String cfg = fedxConfig != null && fedxConfig.length == 1 ? fedxConfig[0] : null;
		instance.init(cfg);

		//////////////////////////////////////////////////////////////////
		//////////// Added Content for SAFE /////////////////////////////

		instance.loadSafeSpefication();
		instance.loadSource_PredicateSets();

		/////////////////////////////////////////////////////////////////
	}

	//////////////////////////////////////////////////////////////////////////////////
	/////////////////////// Added Content for SAFE
	////////////////////////////////////////////////////////////////////////////////// ///////////////////////////////////

	/**
	 * Load SAFE specification file to repository
	 */
	public void loadSafeSpefication() {
		File fileDir = new File("." + File.separator);
		Repository myRepository = new SailRepository(new MemoryStore(fileDir));
		try {
			myRepository.initialize();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		File file = new File(filePath);

		try {
			con = myRepository.getConnection();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		try {
			con.add(file, "hcls.deri.ie", RDFFormat.N3);
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Load predicate, cube sets from settings/specification.n3
	 * 
	 * @throws IOException
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws QueryEvaluationException
	 * @throws RDFParseException
	 */
	public void loadSource_PredicateSets() throws IOException, RepositoryException, MalformedQueryException,
			QueryEvaluationException, RDFParseException {
		loadListC();
		loadSource_UniqueCubes_Graphs(); // source level information
		loadCubeLevelInfo();
	}

	/**
	 * Load cube level information to hashmaps to be used during source
	 * selection
	 * 
	 * @throws MalformedQueryException
	 * @throws RepositoryException
	 * @throws QueryEvaluationException
	 */
	public void loadCubeLevelInfo() throws RepositoryException, MalformedQueryException, QueryEvaluationException {

		String queryString = " prefix safe: <http://safe.sels.insight.org/schema/> " + "Select ?uniquecube ?property" + " WHERE "
				+ " { " + " ?uniquecube safe:uniqueProperty ?property " + "}";
		// System.out.println(queryString);
		TupleQuery tupleQuery = null;
		tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		
		
		while (result.hasNext()) {
			BindingSet bset = result.next();
			String p = bset.getValue("property").stringValue();
			// String [] predPrts =
			// bset.getValue("property").stringValue().split("/");
			// String p= predPrts[predPrts.length-1];
			// if (p.contains("#"))
			// {
			// predPrts = p.split("#");
			// p = predPrts[predPrts.length-1];
			// }
			String uniqueCube = bset.getValue("uniquecube").stringValue();
			uniqueProperty.put(uniqueCube, p);
			
			if(cubeUniquePropertiesMap.containsKey(uniqueCube)) {
				cubeUniquePropertiesMap.get(uniqueCube).add(p);
			} else {
				List<String> uniquePropList = new ArrayList<String>();
				uniquePropList.add(p);
				cubeUniquePropertiesMap.put(uniqueCube, uniquePropList);
			}
		}
		// System.out.println(uniqueProperty);
		queryString = " prefix safe: <http://safe.sels.insight.org/schema/> " + "Select ?cube ?graph" + " WHERE " + " { "
				+ " ?cube safe:graph ?graph " + "}";
		// System.out.println(queryString);
		tupleQuery = null;
		tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		result = tupleQuery.evaluate();

		while (result.hasNext()) {
			BindingSet bset = result.next();
			graph.put(bset.getValue("cube").stringValue(), bset.getValue("graph").stringValue());
		}
		// System.out.println(graph);

	}

	/**
	 * Initialise the sources
	 */
	public void initializeSources() {

	}

	/**
	 * Load List S1 from n3 specification file
	 * 
	 * @throws MalformedQueryException
	 * @throws RepositoryException
	 * @throws QueryEvaluationException
	 */
	public void loadListS1() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		String queryString = getSourceUniquePropertiesQuery("source1");
		TupleQuery tupleQuery = null;
		tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext()) {
			String[] predPrts = result.next().getValue("property").stringValue().split("/");
			String p = predPrts[predPrts.length - 1];
			if (p.contains("#")) {
				predPrts = p.split("#");
				p = predPrts[predPrts.length - 1];
			}
			lstS1.add(p);
		}
		// System.out.println(lstS1);
	}

	/**
	 * load List S2 from n3 specification file
	 * 
	 * @throws QueryEvaluationException
	 * @throws MalformedQueryException
	 * @throws RepositoryException
	 */
	public void loadListS2() throws QueryEvaluationException, RepositoryException, MalformedQueryException {

		String queryString = getSourceUniquePropertiesQuery("source2");
		TupleQuery tupleQuery = null;
		tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext()) {
			String[] predPrts = result.next().getValue("property").stringValue().split("/");
			String p = predPrts[predPrts.length - 1];
			if (p.contains("#")) {
				predPrts = p.split("#");
				p = predPrts[predPrts.length - 1];
			}
			lstS2.add(p);
		}
		// System.out.println(lstS2);
	}

	/**
	 * load List S3 from n3 specification file
	 * 
	 * @throws QueryEvaluationException
	 * @throws MalformedQueryException
	 * @throws RepositoryException
	 */
	public void loadListS3() throws QueryEvaluationException, RepositoryException, MalformedQueryException {

		String queryString = getSourceUniquePropertiesQuery("source3");
		TupleQuery tupleQuery = null;
		tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext()) {
			String[] predPrts = result.next().getValue("property").stringValue().split("/");
			String p = predPrts[predPrts.length - 1];
			if (p.contains("#")) {
				predPrts = p.split("#");
				p = predPrts[predPrts.length - 1];
			}
			lstS3.add(p);
		}
		// System.out.println(lstS3);
	}

	/**
	 * load common properties list C from n3 distribution file
	 * 
	 * @throws QueryEvaluationException
	 * @throws MalformedQueryException
	 * @throws RepositoryException
	 */
	public void loadListC() throws QueryEvaluationException, RepositoryException, MalformedQueryException {

		String queryString = getCommonProperties();
		TupleQuery tupleQuery = null;
		tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext()) {
			String p = result.next().getValue("commonProperties").stringValue();
			// String [] predPrts =
			// result.next().getValue("commonProperties").stringValue().split("/");
			// String p= predPrts[predPrts.length-1];
			// if (p.contains("#"))
			// {
			// predPrts = p.split("#");
			// p = predPrts[predPrts.length-1];
			// }
			lstC.add(p);

		}
		// System.out.println(lstC);
	}

	/**
	 * Load list of unique cubes, Properties and Graphs per data source into
	 * hashmap from n3 distribution file
	 * 
	 * @throws QueryEvaluationException
	 * @throws MalformedQueryException
	 * @throws RepositoryException
	 */
	public void loadSource_UniqueCubes_Graphs()
			throws QueryEvaluationException, RepositoryException, MalformedQueryException {
		// ArrayList<String> endPoints = new ArrayList<String>();
		String queryString = "prefix safe: <http://safe.sels.insight.org/schema/>" + " Select ?endpointurl ?source " + "WHERE  { "
				+ "?source safe:endpointUrl ?endpointurl " + "}";
		TupleQuery tupleQuery = null;
		tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while (result.hasNext()) {
			BindingSet bset = result.next();
			endPoints.add(bset.getValue("endpointurl").stringValue());
		}
		// System.out.println(endPoints);
		// System.out.println(source1 + source2 + source3);
		for (int i = 0; i < endPoints.size(); i++) {
			queryString = "prefix safe: <http://safe.sels.insight.org/schema/>" + " Select ?uniquecubes " + "WHERE  { "
					+ "?source safe:endpointUrl <" + endPoints.get(i) + ">." + "?source safe:uniqueCubes ?uniquecubes "
					+ "}";
			// System.out.println(queryString);
			tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			result = tupleQuery.evaluate();
			ArrayList<String> endPointUniqueCubes = new ArrayList<String>();
			while (result.hasNext()) {
				endPointUniqueCubes.add(result.next().getValue("uniquecubes").toString());
				// System.out.println(result.next().getValue("uniquecubes").toString());
			}
			uniqueCubes.put(endPoints.get(i), endPointUniqueCubes);
			// ---------- load graphs per source into hashmap------------
			queryString = "prefix safe: <http://safe.sels.insight.org/schema/>" + " Select ?graphs " + "WHERE  { "
					+ "?source safe:endpointUrl <" + endPoints.get(i) + ">." + "?source safe:cubes ?cube. "
					+ "?cube safe:graph ?graphs " + "}";
			// System.out.println(queryString);
			tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			result = tupleQuery.evaluate();
			ArrayList<String> endPointGraphs = new ArrayList<String>();
			while (result.hasNext()) {
				endPointGraphs.add(result.next().getValue("graphs").toString());
			}
			graphs.put(endPoints.get(i), endPointGraphs);
			// -------load unique properties per source into
			// hashmap---------------
			queryString = "prefix safe: <http://safe.sels.insight.org/schema/>" + " Select ?uniqueProperties " + "WHERE  { "
					+ "?source safe:endpointUrl <" + endPoints.get(i) + ">."
					+ "?source safe:uniqueProperties ?uniqueProperties. " +

			"}";
			// System.out.println(queryString);
			tupleQuery = Config.con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			result = tupleQuery.evaluate();
			Set<String> srcUniqueProperties = new HashSet<String>();
			while (result.hasNext()) {
				String p = result.next().getValue("uniqueProperties").stringValue();
				// String [] predPrts =
				// result.next().getValue("uniqueProperties").stringValue().split("/");
				// String p= predPrts[predPrts.length-1];
				// if (p.contains("#"))
				// {
				// predPrts = p.split("#");
				// p = predPrts[predPrts.length-1];
				// }
				srcUniqueProperties.add(p);
				allUniqueProperties.add(p);
			}
			uniqueProperties.put(endPoints.get(i), srcUniqueProperties);

		}
		// System.out.println(uniqueCubes);
		// System.out.println(graphs);
		// System.out.println( uniqueProperties);
		// System.out.println( Sets.symmetricDifference(allUniqueProperties,
		// uniqueProperties.get("http://localhost:8892/sparql")));

	}

	/**
	 * Get a SPARQL query that retrieves a set of unique properties for a given
	 * source
	 * 
	 * @param source
	 *            name of the source
	 * @return SPARQL query
	 */
	private String getSourceUniquePropertiesQuery(String source) {
		String qry = "prefix safe: <http://safe.sels.insight.org/schema/>" + " Select ?property " + "WHERE  {"
				+ "<http://safe.sels.insight.org/" + source + "> safe:uniqueProperties ?property " + "}";
		return qry;
	}

	/**
	 * 
	 * A SPARQL query to retrieve the list of common properties between sources
	 * 
	 * @return SPARQL query
	 */
	public String getCommonProperties() {
		String qry = "prefix safe: <http://safe.sels.insight.org/schema/>" + "SELECT ?commonProperties " + "  Where { "
				+ " <http://safe.sels.insight.org/sources/common> safe:commonProperties ?commonProperties " + "}";
		return qry;
	}

	/////////////////////////////////////////////////////////////////////////////////

	private Properties props;

	private Config() {
		props = new Properties();
	}

	private void init(String configFile) throws FedXException {
		if (configFile == null) {
			log.warn("No configuration file specified. Using default config initialization.");
			return;
		}
		log.info("FedX Configuration initialized from file '" + configFile + "'.");
		try {
			FileInputStream in = new FileInputStream(configFile);
			props.load(in);
			in.close();
		} catch (IOException e) {
			throw new FedXException(
					"Failed to initialize FedX configuration with " + configFile + ": " + e.getMessage());
		}
	}

	public String getProperty(String propertyName) {
		return props.getProperty(propertyName);
	}

	public String getProperty(String propertyName, String def) {
		return props.getProperty(propertyName, def);
	}

	/**
	 * the base directory for any location used in fedx, e.g. for repositories
	 * 
	 * @return
	 */
	public String getBaseDir() {
		return props.getProperty("baseDir", "");
	}

	/**
	 * The location of the dataConfig.
	 * 
	 * @return
	 */
	public String getDataConfig() {
		return props.getProperty("dataConfig");
	}

	/**
	 * The location of the cache, i.e. currently used in {@link MemoryCache}
	 * 
	 * @return
	 */
	public String getCacheLocation() {
		return props.getProperty("cacheLocation", "cache.db");
	}

	/**
	 * The number of join worker threads used in the
	 * {@link ControlledWorkerScheduler} for join operations. Default is 20.
	 * 
	 * @return
	 */
	public int getJoinWorkerThreads() {
		return Integer.parseInt(props.getProperty("joinWorkerThreads", "20"));
	}

	/**
	 * The number of join worker threads used in the
	 * {@link ControlledWorkerScheduler} for join operations. Default is 20
	 * 
	 * @return
	 */
	public int getUnionWorkerThreads() {
		return Integer.parseInt(props.getProperty("unionWorkerThreads", "20"));
	}

	/**
	 * The block size for a bound join, i.e. the number of bindings that are
	 * integrated in a single subquery. Default is 15.
	 * 
	 * @return
	 */
	public int getBoundJoinBlockSize() {
		return Integer.parseInt(props.getProperty("boundJoinBlockSize", "15"));
	}

	/**
	 * Get the maximum query time in seconds used for query evaluation. Applied
	 * in CLI or in general if {@link QueryManager} is used to create queries.
	 * <p>
	 * 
	 * Set to 0 to disable query timeouts.
	 * 
	 * @return
	 */
	public int getEnforceMaxQueryTime() {
		return Integer.parseInt(props.getProperty("enforceMaxQueryTime", "30"));
	}

	/**
	 * Flag to enable/disable monitoring features. Default=false.
	 * 
	 * @return
	 */
	public boolean isEnableMonitoring() {
		return Boolean.parseBoolean(props.getProperty("enableMonitoring", "false"));
	}

	/**
	 * Flag to enable/disable JMX monitoring. Default=false
	 * 
	 * @return
	 */
	public boolean isEnableJMX() {
		return Boolean.parseBoolean(props.getProperty("monitoring.enableJMX", "false"));
	}

	/**
	 * Flag to enable/disable query plan logging via {@link QueryPlanLog}.
	 * Default=false The {@link QueryPlanLog} facility allows to retrieve the
	 * query execution plan from a variable local to the executing thread.
	 * 
	 * @return
	 */
	public boolean isLogQueryPlan() {
		return Boolean.parseBoolean(props.getProperty("monitoring.logQueryPlan", "false"));
	}

	/**
	 * Flag to enable/disable query logging via {@link QueryLog}. Default=false
	 * The {@link QueryLog} facility allows to log all queries to a file. See
	 * {@link QueryLog} for details.
	 * 
	 * @return
	 */
	public boolean isLogQueries() {
		return Boolean.parseBoolean(props.getProperty("monitoring.logQueries", "false"));
	}

	/**
	 * Returns the path to a property file containing prefix declarations as
	 * "namespace=prefix" pairs (one per line).
	 * <p>
	 * Default: no prefixes are replaced. Note that prefixes are only replaced
	 * when using the CLI or the {@link QueryManager} to create/evaluate
	 * queries.
	 * 
	 * Example:
	 * 
	 * <code>
	 * foaf=http://xmlns.com/foaf/0.1/
	 * rdf=http://www.w3.org/1999/02/22-rdf-syntax-ns#
	 * =http://mydefaultns.org/
	 * </code>
	 * 
	 * @return
	 */
	public String getPrefixDeclarations() {
		return props.getProperty("prefixDeclarations");
	}

	/**
	 * Returns the fully qualified class name of the
	 * {@link FederationEvalStrategy} implementation that is used in the case of
	 * SAIL implementations, e.g. for native stores.
	 * 
	 * Default {@link SailFederationEvalStrategy}
	 * 
	 * @return
	 */
	public String getSailEvaluationStrategy() {
		return props.getProperty("sailEvaluationStrategy", SailFederationEvalStrategy.class.getName());
	}

	/**
	 * Returns the fully qualified class name of the
	 * {@link FederationEvalStrategy} implementation that is used in the case of
	 * SPARQL implementations, e.g. SPARQL repository or remote repository.
	 * 
	 * Default {@link SparqlFederationEvalStrategy}
	 * 
	 * Alternative implementation:
	 * {@link SparqlFederationEvalStrategyWithValues}
	 * 
	 * @return
	 */
	public String getSPARQLEvaluationStrategy() {
		return props.getProperty("sparqlEvaluationStrategy", SparqlFederationEvalStrategy.class.getName());
	}

	/**
	 * Returns a flag indicating whether vectored evaluation using the VALUES
	 * clause shall be applied for SERVICE expressions.
	 * 
	 * Default: false
	 * 
	 * Note: for todays endpoints it is more efficient to disable vectored
	 * evaluation of SERVICE.
	 * 
	 * @return
	 */
	public boolean getEnableServiceAsBoundJoin() {
		return Boolean.parseBoolean(props.getProperty("optimizer.enableServiceAsBoundJoin", "false"));
	}

	/**
	 * If enabled, repository connections are validated by
	 * {@link ProviderUtil#checkConnectionIfConfigured(org.openrdf.repository.Repository)}
	 * prior to adding the endpoint to the federation. If validation fails, an
	 * error is thrown to the user.
	 * 
	 * @return
	 */
	public boolean isValidateRepositoryConnections() {
		return Boolean.parseBoolean(props.getProperty("validateRepositoryConnections", "true"));
	}

	/**
	 * The debug mode for worker scheduler, the scheduler prints usage stats
	 * regularly if enabled
	 * 
	 * @return false
	 */
	public boolean isDebugWorkerScheduler() {
		return Boolean.parseBoolean(props.getProperty("debugWorkerScheduler", "false"));
	}

	/**
	 * The debug mode for query plan. If enabled, the query execution plan is
	 * printed to stdout
	 * 
	 * @return false
	 */
	public boolean isDebugQueryPlan() {
		return Boolean.parseBoolean(props.getProperty("debugQueryPlan", "false"));
	}

	/**
	 * Set some property at runtime
	 * 
	 * @param key
	 * @param value
	 */
	public void set(String key, String value) {
		props.setProperty(key, value);
	}
}
