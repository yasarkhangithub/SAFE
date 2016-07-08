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

package com.fluidops.fedx.evaluation.union;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryConnection;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ParallelExecutor;
import com.fluidops.fedx.evaluation.concurrent.ParallelTask;
import com.fluidops.fedx.structures.Endpoint;

/**
 * A task implementation representing a prepared union, i.e. the prepared query is executed
 * on the provided triple source.
 * 
 * @author Andreas Schwarte
 */
public class ParallelPreparedAlgebraUnionTask implements ParallelTask<BindingSet> {
	
	protected final TripleSource tripleSource;
	protected final RepositoryConnection conn;
	protected final TupleExpr preparedQuery;
	protected final BindingSet bindings;
	protected final ParallelExecutor<BindingSet> unionControl;
	protected final FilterValueExpr filterExpr;
	
	public ParallelPreparedAlgebraUnionTask(ParallelExecutor<BindingSet> unionControl, TupleExpr preparedQuery, TripleSource tripleSource, RepositoryConnection conn, BindingSet bindings, FilterValueExpr filterExpr) {
		this.preparedQuery = preparedQuery;
		this.bindings = bindings;
		this.unionControl = unionControl;
		this.tripleSource = tripleSource;
		this.conn = conn;
		this.filterExpr = filterExpr;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> performTask() throws Exception {
		return tripleSource.getStatements(preparedQuery, conn, bindings, filterExpr);
	}


	@Override
	public ParallelExecutor<BindingSet> getControl() {
		return unionControl;
	}

	public String toString() {
		Endpoint e = EndpointManager.getEndpointManager().getEndpoint(conn);
		return this.getClass().getSimpleName() + " @" + e.getId() + ": " + preparedQuery.toString();
	}
}
