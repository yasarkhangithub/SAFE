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

package com.fluidops.fedx.optimizer;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;

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


/**
 * Perform source selection during optimization 
 * 
 * @author Andreas Schwarte
 *
 */
public class SourceSelection {

	public static Logger log = Logger.getLogger(SourceSelection.class);
	
	protected final List<Endpoint> endpoints;
	protected final Cache cache;
	protected final QueryInfo queryInfo;
	
	
	public SourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
		this.endpoints = endpoints;
		this.cache = cache;
		this.queryInfo = queryInfo;
	}


	/**
	 * Map statements to their sources. Use synchronized access!
	 */
	protected Map<StatementPattern, List<StatementSource>> stmtToSources = new ConcurrentHashMap<StatementPattern, List<StatementSource>>();
	
	
	/**
	 * Perform source selection for the provided statements using cache or remote ASK queries.
	 * 
	 * Remote ASK queries are evaluated in parallel using the concurrency infrastructure of FedX. Note,
	 * that this method is blocking until every source is resolved.
	 * 
	 * The statement patterns are replaced by appropriate annotations in this optimization.
	 * 
	 * @param stmts
	 */
	public void doSourceSelection(List<StatementPattern> stmts) {
		
		List<CheckTaskPair> remoteCheckTasks = new ArrayList<CheckTaskPair>();
		
		// for each statement determine the relevant sources
		for (StatementPattern stmt : stmts) {
			
			stmtToSources.put(stmt, new ArrayList<StatementSource>());
			
			SubQuery q = new SubQuery(stmt);
				
			// check for each current federation member (cache or remote ASK)
			for (Endpoint e : endpoints) {
				StatementSourceAssurance a = cache.canProvideStatements(q, e);
				if (a==StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
					addSource(stmt, new StatementSource(e.getId(), StatementSourceType.LOCAL));
				} else if (a==StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
					addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));			
				} else if (a==StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {					
					remoteCheckTasks.add( new CheckTaskPair(e, stmt));
				}
			}
		}
		
		// if remote checks are necessary, execute them using the concurrency
		// infrastructure and block until everything is resolved
		if (remoteCheckTasks.size()>0) {
			SourceSelectionExecutorWithLatch.run(this, remoteCheckTasks, cache);
		}
				
		for (StatementPattern stmt : stmtToSources.keySet()) {
			
			List<StatementSource> sources = stmtToSources.get(stmt);
			
			// if more than one source -> StatementSourcePattern
			// exactly one source -> OwnedStatementSourcePattern
			// otherwise: No resource seems to provide results
			
			if (sources.size()>1) {
				StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, queryInfo);
				for (StatementSource s : sources)
					stmtNode.addStatementSource(s);
				stmt.replaceWith(stmtNode);
			}
		
			else if (sources.size()==1) {
				stmt.replaceWith( new ExclusiveStatement(stmt, sources.get(0), queryInfo));
			}
			
			else {
				if (log.isDebugEnabled())
					log.debug("Statement " + QueryStringUtil.toString(stmt) + " does not produce any results at the provided sources, replacing node with EmptyStatementPattern." );
				stmt.replaceWith( new EmptyStatementPattern(stmt));
			}
		}		
	}
	
	/**
	 * Retrieve a set of relevant sources for this query.
	 * @return
	 */
	public Set<Endpoint> getRelevantSources() {
		Set<Endpoint> endpoints = new HashSet<Endpoint>();
		for (List<StatementSource> sourceList : stmtToSources.values())
			for (StatementSource source : sourceList)
				endpoints.add( EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID()));
		return endpoints;
	}	
	
	/**
	 * Add a source to the given statement in the map (synchronized through map)
	 * 
	 * @param stmt
	 * @param source
	 */
	protected void addSource(StatementPattern stmt, StatementSource source) {
		// The list for the stmt mapping is already initialized
		List<StatementSource> sources = stmtToSources.get(stmt);
		synchronized (sources) {
			sources.add(source);
		}
	}
	
	
	
	protected static class SourceSelectionExecutorWithLatch implements ParallelExecutor<BindingSet> {
		
		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch.
		 * Results are added to the map of the source selection instance. Errors 
		 * are reported as {@link OptimizationException} instances.
		 * 
		 * @param tasks
		 */
		public static void run(SourceSelection sourceSelection, List<CheckTaskPair> tasks, Cache cache) {
			new SourceSelectionExecutorWithLatch(sourceSelection).executeRemoteSourceSelection(tasks, cache);
		}		
		
		private final SourceSelection sourceSelection;
		private ControlledWorkerScheduler<BindingSet> scheduler = FederationManager.getInstance().getJoinScheduler();
		private CountDownLatch latch;
		private boolean finished=false;
		private Thread initiatorThread;
		protected List<Exception> errors = new ArrayList<Exception>();
		

		private SourceSelectionExecutorWithLatch(SourceSelection sourceSelection) {
			this.sourceSelection = sourceSelection;
		}

		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch
		 * 
		 * @param tasks
		 */
		private void executeRemoteSourceSelection(List<CheckTaskPair> tasks, Cache cache) {
			if (tasks.size()==0)
				return;
			
			initiatorThread = Thread.currentThread();
			latch = new CountDownLatch(tasks.size());
			for (CheckTaskPair task : tasks)
				scheduler.schedule( new ParallelCheckTask(task.e, task.t, this) );
			
			try	{
				latch.await(); 	// TODO maybe add timeout here
			} catch (InterruptedException e) {
				log.debug("Error during source selection. Thread got interrupted.");
			}

			finished = true;
			
			// check for errors:
			if (errors.size()>0) {
				log.error(errors.size() + " errors were reported:");
				for (Exception e : errors)
					log.error(ExceptionUtil.getExceptionString("Error occured", e));
								
				Exception ex = errors.get(0);
				errors.clear();
				if (ex instanceof OptimizationException)
					throw (OptimizationException)ex;
				
				throw new OptimizationException(ex.getMessage(), ex);
			}
		}

		@Override
		public void run() { /* not needed */ }

		@Override
		public void addResult(CloseableIteration<BindingSet, QueryEvaluationException> res)	{
			latch.countDown();
		}

		@Override
		public void toss(Exception e) {
			errors.add(e);
			scheduler.abort(getQueryId());	// abort all tasks belonging to this query id
			if (initiatorThread!=null)
				initiatorThread.interrupt();
		}

		@Override
		public void done()	{ /* not needed */ }

		@Override
		public boolean isFinished()	{
			return finished;
		}

		@Override
		public int getQueryId()	{
			return sourceSelection.queryInfo.getQueryID();
		}
	}
	
	
	protected class CheckTaskPair {
		public final Endpoint e;
		public final StatementPattern t;
		public CheckTaskPair(Endpoint e, StatementPattern t){
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

				SourceSelection sourceSelection = control.sourceSelection;
				CacheEntry entry = CacheUtils.createCacheEntry(endpoint, hasResults);
				sourceSelection.cache.updateEntry( new SubQuery(stmt), entry);

				if (hasResults)
					sourceSelection.addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));
				
				return null;
			} catch (Exception e) {
				this.control.toss(e);
				throw new OptimizationException("Error checking results for endpoint " + endpoint.getId() + ": " + e.getMessage(), e);
			}
		}

		@Override
		public ParallelExecutor<BindingSet> getControl() {
			return control;
		}		
	}
	
		
}




