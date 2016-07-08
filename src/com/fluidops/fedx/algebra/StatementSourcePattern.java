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

package com.fluidops.fedx.algebra;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.iterator.SingleBindingSetIteration;
import com.fluidops.fedx.evaluation.union.ParallelPreparedUnionTask;
import com.fluidops.fedx.evaluation.union.ParallelUnionTask;
import com.fluidops.fedx.evaluation.union.WorkerUnionBase;
import com.fluidops.fedx.exception.IllegalQueryException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.util.QueryStringUtil;



/**
 * Represents statements that can produce results at a some particular endpoints, the statement sources.
 * 
 * @author Andreas Schwarte
 * 
 * @see StatementSource
 */
public class StatementSourcePattern extends FedXStatementPattern {
		
	
	protected boolean usePreparedQuery = false;
	
	
	public StatementSourcePattern(StatementPattern node, QueryInfo queryInfo) {
		super(node, queryInfo);	
	}			
	
	public void addStatementSource(StatementSource statementSource) {
		statementSources.add(statementSource);		
	}	

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) throws QueryEvaluationException {
					
		try {
			
			Boolean isEvaluated = false;	// is filter evaluated in prepared query
			String preparedQuery = null;	// used for some triple sources
			WorkerUnionBase<BindingSet> union = FederationManager.getInstance().createWorkerUnion(queryInfo);
			
			for (StatementSource source : statementSources) {
				
				Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
				RepositoryConnection conn = ownedEndpoint.getConn();
				TripleSource t = ownedEndpoint.getTripleSource();
				
				/*
				 * Implementation note: for some endpoint types it is much more efficient to use prepared queries
				 * as there might be some overhead (obsolete optimization) in the native implementation. This
				 * is for instance the case for SPARQL connections. In contrast for NativeRepositories it is
				 * much more efficient to use getStatements(subj, pred, obj) instead of evaluating a prepared query.
				 */
				
				if (t.usePreparedQuery()) {
					
					// queryString needs to be constructed only once for a given bindingset
					if (preparedQuery==null) {
						try {
							preparedQuery = QueryStringUtil.selectQueryString(this, bindings, filterExpr, isEvaluated);
						} catch (IllegalQueryException e1) {
							/* all vars are bound, this must be handled as a check query, can occur in joins */
							return handleStatementSourcePatternCheck(bindings);
						}
					}
					 
					union.addTask(new ParallelPreparedUnionTask(union, preparedQuery, t, conn, bindings, (isEvaluated ? null : filterExpr)));
					
				} else {
					union.addTask(new ParallelUnionTask(union, this, t, conn, bindings, filterExpr));
				}
				
			}
			
			union.run();	// execute the union in this thread
			
			return union;
			
		} catch (RepositoryException e) {
			throw new QueryEvaluationException(e);
		} catch (MalformedQueryException e) {
			throw new QueryEvaluationException(e);
		}		
	}
	
	
	protected CloseableIteration<BindingSet, QueryEvaluationException> handleStatementSourcePatternCheck(BindingSet bindings) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		
		// if at least one source has statements, we can return this binding set as result
		
		// XXX do this in parallel for the number of endpoints ?
		for (StatementSource source : statementSources) {
			Endpoint ownedEndpoint = EndpointManager.getEndpointManager().getEndpoint(source.getEndpointID());
			RepositoryConnection ownedConnection = ownedEndpoint.getConn();
			TripleSource t = ownedEndpoint.getTripleSource();
			if (t.hasStatements(this, ownedConnection, bindings))
				return new SingleBindingSetIteration(bindings);
		}
		
		return new EmptyIteration<BindingSet, QueryEvaluationException>();
	}
}
