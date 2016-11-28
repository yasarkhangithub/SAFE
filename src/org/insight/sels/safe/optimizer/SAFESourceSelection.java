
package org.insight.sels.safe.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.insight.sels.safe.util.SAFEUtility;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;
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

		// partitions the BGP into sub-BGPs connected by s-s joins
		// (joins can be on constants or variables, predicates can be bound or not ...
		// it doesn't matter).
		PartitionBGPBySSJoins partionedBgp = new PartitionBGPBySSJoins(stmts);

		// for each such partition
		for(List<StatementPattern> partition:partionedBgp.getPartition()){
//			System.out.println("Partition Started --------------------------------- ");
			// store all properties in this partition
			Set<String> partAllProperties = new TreeSet<String>();
			for(StatementPattern sp: partition){
//				System.out.println(sp.toString());
				stmtToGraphs.put(sp, new ArrayList<String>());
				stmtToSources.put(sp, new ArrayList<StatementSource>());
//				SubQuery q = new SubQuery(sp);
				
				if(sp.getPredicateVar().getValue()!=null) // if bound
					partAllProperties.add(sp.getPredicateVar().getValue().stringValue());
				// otherwise if not bound, will not affect matches
			}
//			System.out.println("Partition End ------------------------------------ ");
			// will store patterns with non-bound predicate that do not
			// have a unique graph so we can use ASK queries for those later
			ArrayList<StatementPattern> nonUniqueNonBoundStmts = new ArrayList<StatementPattern>();
			
			for (Endpoint e : endpoints) {
				String src = e.getEndpoint();
				
				// we don't really care here which properties are unique or not
				// just need the list of all properties
				Set<String> srcUniqueProperties = Config.uniqueProperties.get(src);
				Set<String> srcAllProperties = Sets.union(srcUniqueProperties, Config.lstC);
				
				if(srcAllProperties.containsAll(partAllProperties)){
					// would be ideal to have the list of all properties per graph
					// and check that they contain all the partition properties ... 
					// but instead let's use the unique cubes/properties data we have
					List<String> srcUniqueCubes = Config.uniqueCubes.get(src);
					// a cube that contains a unique property in the partition
					String relevantUniqueCube = null;
					for (int i = 0; i < srcUniqueCubes.size(); i++) {
						String uniqueCube = srcUniqueCubes.get(i);
						List<String> cubeUniqueProperties = Config.cubeUniquePropertiesMap.get(uniqueCube);
						
						// if there's a unique property for that cube in the partition, 
						// remember that cube
						Set<String> cubeUniquePropertiesSet = new TreeSet<String>();
						cubeUniquePropertiesSet.addAll(cubeUniqueProperties);
						if(Sets.intersection(cubeUniquePropertiesSet,partAllProperties).size()>0){
							if(relevantUniqueCube!=null){
								// we found a second cube with a unique property
								// in the same partition ...
								// this means the partition cannot have an answer
								// and nor can the bgp
								return;
							} else{
								// otherwise we found a property unique to this cube
								relevantUniqueCube = uniqueCube;
							}
						}
					}
					
					String id = "sparql_" + src.replace("http://", "").replace("/", "_");
					
					if(relevantUniqueCube!=null){
						// we found one cube with one unique property
						// add just that graph and the corresponding source for
						// all triples in the pattern
						for(StatementPattern stmt:partition){
							addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
							String graph = Config.graph.get(relevantUniqueCube);
							addGraph(stmt, graph);
						}
					} else{
						// need to add all graphs in this source
						// for each triple in the partition
						// unless the predicate is not bound, in which
						// case we can use an ASK (as before)
						for(StatementPattern stmt:partition){
							if(!stmt.getPredicateVar().hasValue()){
								nonUniqueNonBoundStmts.add(stmt);
							} else{
								ArrayList<String> relevantgraphs = getNonUniqueRelevantGraphs(stmt, src);
								if(relevantgraphs.size() > 0) {
									bindGraphsToStatement(stmt, relevantgraphs);
									addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
								}
							}
						}
					}
				}
			}
			
			// okay, so now some triple patterns without predicates bound
			// and without a unique predicate in the group may be left
			// so we revert to ASK for these
			for(StatementPattern stmt:nonUniqueNonBoundStmts){
				// check for each current federation member (cache or remote ASK)
				for (Endpoint e : endpoints) {
					SubQuery q = new SubQuery(stmt);
					StatementSourceAssurance a = cache.canProvideStatements(q, e);
					if (a == StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
						addSource(stmt, new StatementSource(e.getId(), StatementSourceType.LOCAL));
					} else if (a == StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
						addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
					} else if (a == StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {
						remoteCheckTasks.add(new CheckTaskPair(e, stmt));
					}
				}
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
//						 System.out.println("------------\n"+stmt);
//						 System.out.println(sources);
//						 System.out.println("------------ Graphs-----");
//						 List<String> graphs = stmtToGraphs.get(stmt);
//						 System.out.println(graphs);

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


	public ArrayList<String> getNonUniqueRelevantGraphs(StatementPattern stmt, String src) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		ArrayList<String> relevantGraphs = new ArrayList<String>();

		Repository repo = new SPARQLRepository(src);
		repo.initialize();

		String query = QueryStringUtil.selectQueryStringForNG(stmt, EmptyBindingSet.getInstance());
		//		System.out.println(query);
		TupleQuery selectQuery = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, query);
		TupleQueryResult res = selectQuery.evaluate();
		while(res.hasNext()) {
			BindingSet bSet = res.next();
			String graph = bSet.getBinding("g").getValue().toString();
			//			System.out.println(graph);
			relevantGraphs.add(graph);
		}

		//		for (String graph : allGraphs) {
		//			
		//			String query = QueryStringUtil.askQueryString(stmt, EmptyBindingSet.getInstance(), graph);
		//			
		//			
		//			System.out.println(query);
		//			
		//			BooleanQuery askQuery = repo.getConnection().prepareBooleanQuery(QueryLanguage.SPARQL, query);
		//			if(askQuery.evaluate()) {
		//				relevantGraphs.add(graph);
		//			}
		//		}
		//		System.out.println("Source: " + src);
		//		System.out.println("All Graphs: " + allGraphs.size() + " === Selected Graphs: " + relevantGraphs.size());

		return relevantGraphs;

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

	public static class PartitionBGPBySSandSOJoins {
		/**
		 * Partition the query into patterns joined (recursively) 
		 * by s-s joins or s-o joins
		 */
		final Map<String,List<Integer>> subjectToStatements;
		final Map<String,List<Integer>> objectToStatements;

		final List<List<StatementPattern>> partitions;
		final Map<Integer,TreeSet<Integer>> idToPartition;

		public PartitionBGPBySSandSOJoins(List<StatementPattern> bgp){
			partitions = new ArrayList<List<StatementPattern>>();
			subjectToStatements = new HashMap<String,List<Integer>>();
			objectToStatements = new HashMap<String,List<Integer>>();
			idToPartition = new HashMap<Integer,TreeSet<Integer>>();

			partition(bgp);
		}

		public List<List<StatementPattern>> getPartition() {
			return partitions;
		}

		private void partition(List<StatementPattern> bgp){
			// need to work with IDs to help ensure 
			// uniqueness in final partition (plus I don't 
			// know that StatementPattern.hashCode() works
			// nor what equals does so ...)
			for(int i=0; i<bgp.size(); i++){
				StatementPattern sp = bgp.get(i);
				add(subjectToStatements,sp.getSubjectVar(),i);
				add(objectToStatements,sp.getObjectVar(),i);
			}

			// each subject group (no matter if constant or variable)
			// forms an initial partition that can only be answered
			// over one graph containing a data cube
			for(List<Integer> subjectGroup : subjectToStatements.values()){
				TreeSet<Integer> subjectGroupSet = new TreeSet<Integer>();
				subjectGroupSet.addAll(subjectGroup);
				for(Integer patternId:subjectGroup){
					idToPartition.put(patternId,subjectGroupSet);
				}
			}

			// all statements now in an initial S-S partition forming subject groups
			// now connect subject groups connected through an s-o join
			// to form larger partitions (again, no matter if constant or variable)
			for(Map.Entry<String,List<Integer>> oToStmts: objectToStatements.entrySet()){
				// get subject group with that object (if any)
				List<Integer> sJoin = subjectToStatements.get(oToStmts.getKey());


				if(sJoin!=null){
					// get the partition the subject is in (can only be in one partition
					// since we already grouped by subject)
					TreeSet<Integer> sPartition = idToPartition.get(sJoin.get(0));

					// how many partitions we are merging
					// we've already added one for the subject group
					int partitionsAdded = 1;

					// get the object partitions and merge them with the subject
					// partition ...
					// there may be multiple object partitions since we don't want to
					// join partitions by o-o joins but if there's a subject
					// join we can pool object partitions based on an s-o join
					for(Integer oStmt:oToStmts.getValue()){
						if(sPartition.addAll(idToPartition.get(oStmt)))
							partitionsAdded++;
					}

					// map ids of new partition to the partition itself
					// (strictly speaking no need to update subject ids but okay :P)
					if(partitionsAdded>1){
						for(Integer mStmtId:sPartition){
							idToPartition.put(mStmtId,sPartition);
						}
					}
				}
			}

			// map partition ids back to actual patterns
			for(Map.Entry<Integer, TreeSet<Integer>> idToP : idToPartition.entrySet()){
				// just to avoid adding the same partition twice
				// this condition will match each partition exactly once
				if(idToP.getValue().first().equals(idToP.getKey())){
					List<StatementPattern> part = new ArrayList<StatementPattern>();
					for(Integer i:idToP.getValue()){
						part.add(bgp.get(i));
					}
					partitions.add(part);
				}
			}
		}

		/**
		 * Maps subject or object term to list of patterns. Creates new list if empty.
		 * Considers both variables and constants as key.
		 * @param map
		 * @param key
		 * @param patternId
		 * @return
		 */
		public static boolean add(Map<String, List<Integer>> map, Var key, int patternId){
			String keyStr = null;
			if(key.getValue()!=null){
				keyStr = "c_"+key.getValue().stringValue();
			} else{
				keyStr = "v_"+key.getName();
			}

			List<Integer> list = map.get(keyStr);

			if(list==null){
				list = new ArrayList<Integer>();
				map.put(keyStr, list);
			}
			return list.add(patternId);
		}

	}
	
	
	public static class PartitionBGPBySSJoins {
		/**
		 * Partition the query into patterns joined (recursively) 
		 * by s-s joins or s-o joins
		 */
		final Map<String,List<StatementPattern>> subjectToStatements;

		final List<List<StatementPattern>> partitions;

		public PartitionBGPBySSJoins(List<StatementPattern> bgp){
			partitions = new ArrayList<List<StatementPattern>>();
			subjectToStatements = new HashMap<String,List<StatementPattern>>();

			partition(bgp);
		}

		public List<List<StatementPattern>> getPartition() {
			return partitions;
		}

		private void partition(List<StatementPattern> bgp){
			// need to work with IDs to help ensure 
			// uniqueness in final partition (plus I don't 
			// know that StatementPattern.hashCode() works
			// nor what equals does so ...)
			for(int i=0; i<bgp.size(); i++){
				StatementPattern sp = bgp.get(i);
				add(subjectToStatements,sp.getSubjectVar(),sp);
			}

			// each subject group (no matter if constant or variable)
			// forms an initial partition that can only be answered
			// over one graph containing a data cube
			partitions.addAll(subjectToStatements.values());
		}

		/**
		 * Maps subject or object term to list of patterns. Creates new list if empty.
		 * Considers both variables and constants as key.
		 * @param subjectToStatements
		 * @param key
		 * @param sp
		 * @return
		 */
		public static boolean add(Map<String, List<StatementPattern>> subjectToStatements, Var key, StatementPattern sp){
			String keyStr = null;
			if(key.getValue()!=null){
				keyStr = "c_"+key.getValue().stringValue();
			} else{
				keyStr = "v_"+key.getName();
			}

			List<StatementPattern> list = subjectToStatements.get(keyStr);

			if(list==null){
				list = new ArrayList<StatementPattern>();
				subjectToStatements.put(keyStr, list);
			}
			return list.add(sp);
		}

	}

	// quick partitioner test
	//	public static void main(String[] args) throws MalformedQueryException{
	//		String queryString = "SELECT * WHERE "
	//				+ "{ "
	//				+ " ?s <http://ex.org/1> ?o . "
	//				+ " ?s <http://ex.org/2> ?o2 . "
	//				+ " ?s2 ?p1 ?o2 . "
	//				+ " <http://ex.org/3> ?p2 ?s2 . "
	//				+ " <http://ex.org/3> ?p3 ?o2 . "
	//				+ " ?s ?p3 <http://ex.org/3> . "
	//				+ " ?o2 ?p3 ?o3  . "
	//				+ " ?o3 ?p4 ?o4 . "
	//				+ " ?o3 ?p4 ?o5 . "
	//				+ " ?o5 ?p6 ?o7 . "
	//				+ " ?s3 ?p3 ?o . "
	//				+ "}";
	//		ParsedOperation query = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, queryString, null);
	//		if (!(query instanceof ParsedQuery))
	//			throw new MalformedQueryException("Not a ParsedQuery: " + query.getClass());
	//		// we use a dummy query info object here
	//		QueryInfo qInfo = new QueryInfo(queryString, QueryType.SELECT);
	//		TupleExpr tupleExpr = ((ParsedQuery)query).getTupleExpr();
	//		GenericInfoOptimizer info = new GenericInfoOptimizer(qInfo);
	//		
	//		// collect information and perform generic optimizations
	//		info.optimize(tupleExpr);
	//		
	//		PartitionBGPBySSandSOJoins part = new PartitionBGPBySSandSOJoins(info.getStatements());
	//		for(List<StatementPattern> p:part.getPartition()){
	//			System.err.println("###############");
	//			System.err.println(p);
	//			System.err.println("###############");
	//		}
	//	}

}
