
package org.insight.sels.safe.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.insight.sels.safe.util.SAFEUtility;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParseException;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.EmptyStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.cache.CacheEntry;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.concurrent.ParallelExecutor;
import com.fluidops.fedx.evaluation.concurrent.ParallelTask;
import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.SubQuery;
import com.fluidops.fedx.util.QueryStringUtil;
import com.google.common.collect.Sets;

import info.aduna.iteration.CloseableIteration;

/**
 * SAFE source selection
 * 
 * @author saleem, yasar
 *
 */
public class SAFESourceSelection {

	public static Logger log = Logger.getLogger(SAFESourceSelection.class);

	protected final List<Endpoint> endpoints;
	protected final Cache cache;
	protected final QueryInfo queryInfo;
	public String namedGraphs = "";
	public HashMap<Integer, String> namedGraphMap = new HashMap<Integer, String>();
	public Integer graphCounter = 1;

	public SAFESourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
		this.endpoints = endpoints;
		this.cache = cache;
		this.queryInfo = queryInfo;
	}

	/**
	 * Map statements to their sources and graphs. Use synchronized access!
	 */
	public Map<StatementPattern, List<StatementSource>> stmtToSources = new ConcurrentHashMap<StatementPattern, List<StatementSource>>();
	public Map<StatementPattern, List<String>> stmtToGraphs = new ConcurrentHashMap<StatementPattern, List<String>>();

	/**
	 * Sources, predicate Sets
	 */
	protected String pJoinVar; // name of the variable which will be use as path
								// join between tcga:result and other query
								// triples patterns
	protected String qryPatientBarCode; // global patient barcode if specified
										// in the user query
	protected HashMap<String, List<String>> starJoinGrps = new HashMap<String, List<String>>();
	protected HashMap<String, List<String>> pathJoinGrps = new HashMap<String, List<String>>();
	long startTime = System.currentTimeMillis();

	/**
	 * Perform source selection using the predicate, source grouping specified
	 * at settings/specifications.n3 and ASK queries. Remote ASK queries are
	 * only used if only object is bound in a triple pattern.
	 * 
	 * The statement patterns are replaced by appropriate annotations in this
	 * optimization.
	 * 
	 * @param stmts
	 *            list of triple patterns
	 * @throws RepositoryException
	 * @throws QueryEvaluationException
	 * @throws MalformedQueryException
	 * @throws RDFParseException
	 */
	public void doSAFESourceSelection(List<StatementPattern> stmts)
			throws RepositoryException, MalformedQueryException, QueryEvaluationException, RDFParseException {

		List<CheckTaskPair> remoteCheckTasks = new ArrayList<CheckTaskPair>();
		// long startTime = System.currentTimeMillis();
		// loadSource_PredicateSets();
		// long endTime = System.currentTimeMillis();
		// System.out.println("load set Execution time(msec) : "+
		// (endTime-startTime));

		createStarJoinGroups(stmts);
		createPathJoinGroups(stmts);
		// System.out.println(starJoinGrps);
		// System.out.println(pathJoinGrps);
		// for each statement (triple pattern)of user query determine the
		// relevant sources
		for (StatementPattern stmt : stmts) {
			stmtToGraphs.put(stmt, new ArrayList<String>());
			stmtToSources.put(stmt, new ArrayList<StatementSource>());
			SubQuery q = new SubQuery(stmt);

			String s, p;
			if (stmt.getSubjectVar().getValue() != null)
				s = stmt.getSubjectVar().getValue().stringValue();
			else
				s = "null";
			if (stmt.getPredicateVar().getValue() != null)
				p = stmt.getPredicateVar().getValue().stringValue();
			else
				p = "null";

			// ---------------------------------------------
			if (!p.equals("null")) // --- if predicate is bound
			{
				s = stmt.getSubjectVar().getName().toString();
				// Set<String> lstSources= Config.uniqueProperties.keySet();
				// //retrieve list of sources that contains at least one unique
				// property
				for (Endpoint e : endpoints) {
					String src = e.getEndpoint();
					Set<String> srcUniqueProperties = Config.uniqueProperties.get(src);
					Set<String> upc = Sets.union(srcUniqueProperties, Config.lstC); // union of src unique properties and common properties of all sources
					
//					if (upc.contains(p) && (((isStarJoin(stmt, srcUniqueProperties) == true
//							|| isPathJoin(stmt, srcUniqueProperties) == true)
//							|| (isStarJoin(stmt,
//									Sets.symmetricDifference(Config.allUniqueProperties, srcUniqueProperties)) == false
//									&& isPathJoin(stmt, Sets.symmetricDifference(Config.allUniqueProperties,
//											srcUniqueProperties)) == false)))) {
					if (upc.contains(p) && (((isStarJoin(stmt,
										Sets.symmetricDifference(Config.allUniqueProperties, srcUniqueProperties)) == false
										&& isPathJoin(stmt, Sets.symmetricDifference(Config.allUniqueProperties,
												srcUniqueProperties)) == false)))) {
						String id = "sparql_" + src.replace("http://", "").replace("/", "_");
						addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
						
						// --- graph level filtering
						List<String> srcUniqueCubes = Config.uniqueCubes.get(src);
						Boolean isBelongToUniqueSrcGrph = false;
						for (int i = 0; i < srcUniqueCubes.size(); i++) {
							String uniqueProperty = Config.uniqueProperty.get(srcUniqueCubes.get(i));
							List<String> lstSJGrp = starJoinGrps.get(s);
							if (lstSJGrp.contains(uniqueProperty)) {
								String graph = Config.graph.get(srcUniqueCubes.get(i).toString());
								addGraph(stmt, graph);
								isBelongToUniqueSrcGrph = true;
								break;
							}
						}
						
						if (isBelongToUniqueSrcGrph == false) // then select all graphs for the current source
						{
							bindGraphsToStatement(stmt, Config.graphs.get(src));
							// System.out.println(stmt.getPredicateVar()+ "
							// graphs: "+Config.graphs.get(src));
						}
					}

				}

			} else if (p.equals("null")) // --- if only predicate is not bound
			{
				// check for each current federation member (cache or remote ASK)
				for (Endpoint e : endpoints) {
					StatementSourceAssurance a = cache.canProvideStatements(q, e);
					if (a == StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
						addSource(stmt, new StatementSource(e.getId(), StatementSourceType.LOCAL));
					} else if (a == StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
						addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
					} else if (a == StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {
						remoteCheckTasks.add(new CheckTaskPair(e, stmt));
					}
				}
				// System.out.println("object is only bound");
			}

		}

		long endTime = System.currentTimeMillis();
		System.out.println("Source Selection Exe time (msec): " + (endTime - startTime));

		// if remote checks are necessary, execute them using the concurrency
		// infrastructure and block until everything is resolved
		if (remoteCheckTasks.size() > 0) {
			SourceSelectionExecutorWithLatch.run(this, remoteCheckTasks, cache);
			System.out.println("Number of ASK request: " + remoteCheckTasks.size());
		} else
			System.out.println("Number of ASK request: 0");
		int triplePatternWiseSources = 0;

		for (StatementPattern stmt : stmtToSources.keySet()) {

			List<StatementSource> sources = stmtToSources.get(stmt);
			triplePatternWiseSources = triplePatternWiseSources + sources.size();

			// Printing Sources and Graphs selected for each triple pattern
			// System.out.println("------------\n"+stmt);
			// System.out.println(sources);
			// System.out.println("------------ Graphs-----");
			// List<String> graphs = stmtToGraphs.get(stmt);
			// System.out.println(graphs);

			// if more than one source -> StatementSourcePattern
			// exactly one source -> OwnedStatementSourcePattern
			// otherwise: No resource seems to provide results
			if (sources.size() > 1) {
				StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, queryInfo);
				for (StatementSource s : sources)
					stmtNode.addStatementSource(s);
				stmt.replaceWith(stmtNode);
			}

			else if (sources.size() == 1) {
				String modifiedQuery = SAFEUtility.addNamedGraphsToSingleSrcQuery(namedGraphMap, queryInfo.getQuery());
				queryInfo.setQuery(modifiedQuery);
				stmt.replaceWith(new ExclusiveStatement(stmt, sources.get(0), queryInfo));
			}

			else {
				if (log.isDebugEnabled())
					log.debug("Statement " + QueryStringUtil.toString(stmt)
							+ " does not produce any results at the provided sources, replacing node with EmptyStatementPattern.");
				stmt.replaceWith(new EmptyStatementPattern(stmt));
			}
		}
		System.out.println("Total Triple Pattern-wise selected sources: " + triplePatternWiseSources);

	}

	/**
	 * Bind the finally selected sources to the corresponding statment
	 * 
	 * @param stmt
	 *            statement
	 * @param lstFinalSources
	 *            sources to be bind
	 */
	public void bindSourcesToStatement(StatementPattern stmt, ArrayList<String> lstFinalSources) {

		for (String src : lstFinalSources) {
			String id = "sparql_" + src.replace("http://", "").replace("/", "_");
			addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
		}
	}

	/**
	 * Bind the selected graphs to the corresponding statment
	 * 
	 * @param stmt
	 *            statement
	 * @param graphs
	 *            List of graphs to be bind
	 */
	public void bindGraphsToStatement(StatementPattern stmt, ArrayList<String> graphs) {

		for (String src : graphs) {

			addGraph(stmt, src);
		}
	}

	/**
	 * Create star join groups from user query
	 * 
	 * @param stmts
	 *            list of triple patterns in query
	 */

	public void createStarJoinGroups(List<StatementPattern> stmts) {
		for (StatementPattern stmt : stmts) {
			// System.out.println(stmt.getParentNode());
			if (stmt.getSubjectVar().getValue() == null && stmt.getPredicateVar().getValue() != null) // ---
																										// we
																										// are
																										// only
																										// interested
																										// if
																										// subject
																										// is
																										// unbound
																										// and
																										// predicate
																										// is
																										// bound
			{
				String sbjKey = stmt.getSubjectVar().getName().toString();
				String predValue = stmt.getPredicateVar().getValue().toString();
				if (starJoinGrps.containsKey(sbjKey)) {
					List<String> lstPredValues = starJoinGrps.get(sbjKey);
					lstPredValues.add(predValue);
				} else {
					List<String> lstPredValues = new ArrayList<String>();
					lstPredValues.add(predValue);
					starJoinGrps.put(sbjKey, lstPredValues);
				}
			}
		}
	}

	/**
	 * Create path join groups from user query
	 * 
	 * @param stmts
	 *            list of triple patterns in query
	 */

	public void createPathJoinGroups(List<StatementPattern> stmts) {
		ArrayList<String> objkeys = new ArrayList<String>();
		for (StatementPattern stmt : stmts) {
			if (stmt.getObjectVar().getValue() == null && stmt.getPredicateVar().getValue() != null) // ---
																										// we
																										// are
																										// only
																										// interested
																										// if
																										// object
																										// is
																										// unbound
																										// and
																										// predicate
																										// is
																										// bound
			{
				String objKey = stmt.getObjectVar().getName().toString();
				if (!objkeys.contains(objKey))
					objkeys.add(objKey);
			}
		}

		for (StatementPattern stmt : stmts) {
			if (stmt.getSubjectVar().getValue() == null && stmt.getPredicateVar().getValue() != null) // ---
																										// we
																										// are
																										// only
																										// interested
																										// if
																										// subject
																										// is
																										// unbound
																										// and
																										// predicate
																										// is
																										// bound
			{

				String sbjKey = stmt.getSubjectVar().getName().toString();
				String predValue = stmt.getPredicateVar().getValue().toString();
				if (objkeys.contains(sbjKey)) {
					if (pathJoinGrps.containsKey(sbjKey)) {
						List<String> lstPredValues = pathJoinGrps.get(sbjKey);
						lstPredValues.add(predValue);
					} else {
						List<String> lstPredValues = new ArrayList<String>();
						lstPredValues.add(predValue);
						pathJoinGrps.put(sbjKey, lstPredValues);
					}
				}
			}
		}
	}

	/**
	 * Check for path join. But the predicate must have a start join with
	 * predicate result
	 * 
	 * @param s
	 * @param p
	 * @param lstM
	 * @return Boolean
	 */
	// private boolean isPathJoin(String s, String p, ArrayList<String> lstM) {
	// Boolean value=false;
	//
	// if(pJoinVar != null)
	// {
	// @SuppressWarnings({ "unchecked" })
	// List<String>lstSJGrp= (List<String>) starJoinGrps.get(pJoinVar);
	// for(int i=0;i<lstSJGrp.size();i++)
	// {
	// if (lstM.contains(lstSJGrp.get(i)))
	// value = true;
	// }
	// }
	// return value;
	//
	// }

	/**
	 * Check Star join between predicate of a statement pattern and a predicate
	 * set
	 * 
	 * @param stmt
	 *            statement pattern
	 * @param lst
	 *            predicate set
	 * @return boolean
	 */
	private boolean isStarJoin(StatementPattern stmt, Set<String> lst) {
		Boolean value = false;
		String sbj, pred;
		if (stmt.getSubjectVar().getName() != null)
			sbj = stmt.getSubjectVar().getName().toString();
		else
			sbj = "null";
		if (stmt.getPredicateVar().getValue() != null)
			pred = stmt.getPredicateVar().getValue().stringValue();
		else
			pred = "null";

		if (!sbj.equals("null")) {

			List<String> lstSJGrp = (List<String>) starJoinGrps.get(sbj);
			for (int i = 0; i < lstSJGrp.size(); i++) {
				if (lst.contains(lstSJGrp.get(i)) && lstSJGrp.contains(pred)
						&& stmt.getParentNode().toString().contains(lstSJGrp.get(i))) {

					value = true;
				}
			}
		}
		return value;
	}

	/**
	 * Check Path join between predicate of a statement pattern and a predicate
	 * set
	 * 
	 * @param stmt
	 *            statement pattern
	 * @param lst
	 *            predicate set
	 * @return boolean
	 */
	private boolean isPathJoin(StatementPattern stmt, Set<String> lst) {
		Boolean value = false;
		String obj;
		if (stmt.getObjectVar().getName() != null)
			obj = stmt.getObjectVar().getName().toString();
		else
			obj = "null";

		// ------------------------------
		if (!obj.equals("null") && pathJoinGrps.get(obj) != null) {

			List<String> lstGrpPred = pathJoinGrps.get(obj);
			for (int i = 0; i < lstGrpPred.size(); i++) {
				if (lst.contains(lstGrpPred.get(i)) && stmt.getParentNode().toString().contains(lstGrpPred.get(i))) {

					value = true;
				}
			}
		}
		return value;
	}

	/**
	 * Retrieve a set of relevant sources for this query.
	 * 
	 * @return
	 */
	public Set<Endpoint> getRelevantSources() {
		Set<Endpoint> endpoints = new HashSet<Endpoint>();
		for (List<StatementSource> sourceList : stmtToSources.values())
			for (StatementSource source : sourceList)
				endpoints.add(EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID()));
		return endpoints;
	}

	/**
	 * Add a source to the given statement in the map (synchronized through map)
	 * 
	 * @param stmt
	 * @param source
	 */
	public void addSource(StatementPattern stmt, StatementSource source) {
		// The list for the stmt mapping is already initialized
		List<StatementSource> sources = stmtToSources.get(stmt);
		synchronized (sources) {
			sources.add(source);
		}
	}

	/**
	 * Add a graph to the given statement in the map (synchronized through map)
	 * 
	 * @param stmt
	 * @param source
	 */
	public void addGraph(StatementPattern stmt, String graphName) {
		// The list for the stmt mapping is already initialized
		List<String> graphs = stmtToGraphs.get(stmt);
		synchronized (graphs) {
			graphs.add(graphName);
			// System.out.println("Stmt: " + stmt.toString() + " ===== Graph
			// Name: " + graphName);
			if (!(namedGraphMap.containsValue(graphName))) {
				namedGraphMap.put(graphCounter, graphName);
				QueryInfo.setNamedGraphMap(namedGraphMap);
				graphCounter++;
			}
		}
	}

	protected static class SourceSelectionExecutorWithLatch implements ParallelExecutor<BindingSet> {

		/**
		 * Execute the given list of tasks in parallel, and block the thread
		 * until all tasks are completed. Synchronization is achieved by means
		 * of a latch. Results are added to the map of the source selection
		 * instance. Errors are reported as {@link OptimizationException}
		 * instances.
		 * 
		 * @param tasks
		 */
		public static void run(SAFESourceSelection sourceSelection, List<CheckTaskPair> tasks, Cache cache) {
			new SourceSelectionExecutorWithLatch(sourceSelection).executeRemoteSourceSelection(tasks, cache);
		}

		private final SAFESourceSelection sourceSelection;
		private ControlledWorkerScheduler<BindingSet> scheduler = FederationManager.getInstance().getJoinScheduler();
		private CountDownLatch latch;
		private boolean finished = false;
		private Thread initiatorThread;
		protected List<Exception> errors = new ArrayList<Exception>();

		private SourceSelectionExecutorWithLatch(SAFESourceSelection sourceSelection) {
			this.sourceSelection = sourceSelection;
		}

		/**
		 * Execute the given list of tasks in parallel, and block the thread
		 * until all tasks are completed. Synchronization is achieved by means
		 * of a latch
		 * 
		 * @param tasks
		 */
		private void executeRemoteSourceSelection(List<CheckTaskPair> tasks, Cache cache) {
			if (tasks.size() == 0)
				return;

			initiatorThread = Thread.currentThread();
			latch = new CountDownLatch(tasks.size());
			for (CheckTaskPair task : tasks)
				scheduler.schedule(new ParallelCheckTask(task.e, task.t, this));

			try {
				latch.await(); // TODO maybe add timeout here
			} catch (InterruptedException e) {
				log.debug("Error during source selection. Thread got interrupted.");
			}

			finished = true;

			// check for errors:
			if (errors.size() > 0) {
				log.error(errors.size() + " errors were reported:");
				for (Exception e : errors)
					log.error(ExceptionUtil.getExceptionString("Error occured", e));

				Exception ex = errors.get(0);
				errors.clear();
				if (ex instanceof OptimizationException)
					throw (OptimizationException) ex;

				throw new OptimizationException(ex.getMessage(), ex);
			}
		}

		@Override
		public void run() {
			/* not needed */ }

		@Override
		public void addResult(CloseableIteration<BindingSet, QueryEvaluationException> res) {
			latch.countDown();
		}

		@Override
		public void toss(Exception e) {
			errors.add(e);
			scheduler.abort(getQueryId()); // abort all tasks belonging to this
											// query id
			if (initiatorThread != null)
				initiatorThread.interrupt();
		}

		@Override
		public void done() {
			/* not needed */ }

		@Override
		public boolean isFinished() {
			return finished;
		}

		@Override
		public int getQueryId() {
			return sourceSelection.queryInfo.getQueryID();
		}
	}

	public class CheckTaskPair {
		public final Endpoint e;
		public final StatementPattern t;

		public CheckTaskPair(Endpoint e, StatementPattern t) {
			this.e = e;
			this.t = t;
		}
	}

	/**
	 * Task for sending an ASK request to the endpoints (for source selection)
	 * 
	 * @author Andreas Schwarte
	 */
	protected static class ParallelCheckTask implements ParallelTask<BindingSet> {

		protected final Endpoint endpoint;
		protected final StatementPattern stmt;
		protected final SourceSelectionExecutorWithLatch control;

		public ParallelCheckTask(Endpoint endpoint, StatementPattern stmt, SourceSelectionExecutorWithLatch control) {
			this.endpoint = endpoint;
			this.stmt = stmt;
			this.control = control;
		}

		@Override
		public CloseableIteration<BindingSet, QueryEvaluationException> performTask() throws Exception {
			try {
				TripleSource t = endpoint.getTripleSource();
				RepositoryConnection conn = endpoint.getConn();

				boolean hasResults = t.hasStatements(stmt, conn, EmptyBindingSet.getInstance());

				SAFESourceSelection sourceSelection = control.sourceSelection;
				CacheEntry entry = CacheUtils.createCacheEntry(endpoint, hasResults);
				sourceSelection.cache.updateEntry(new SubQuery(stmt), entry);

				if (hasResults)
					sourceSelection.addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));

				return null;
			} catch (Exception e) {
				this.control.toss(e);
				throw new OptimizationException(
						"Error checking results for endpoint " + endpoint.getId() + ": " + e.getMessage(), e);
			}
		}

		@Override
		public ParallelExecutor<BindingSet> getControl() {
			return control;
		}
	}

}
