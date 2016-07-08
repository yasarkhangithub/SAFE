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
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.repository.RepositoryConnection;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ParallelExecutor;
import com.fluidops.fedx.evaluation.concurrent.ParallelTask;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.util.QueryStringUtil;

/**
 * A task implementation representing a statement expression to be evaluated.
 * 
 * @author Andreas Schwarte
 */
public class ParallelUnionTask implements ParallelTask<BindingSet> {
	
	protected final TripleSource tripleSource;
	protected final RepositoryConnection conn;
	protected final StatementPattern stmt;
	protected final BindingSet bindings;
	protected final ParallelExecutor<BindingSet> unionControl;
	protected final FilterValueExpr filterExpr;
	
	public ParallelUnionTask(ParallelExecutor<BindingSet> unionControl, StatementPattern stmt, TripleSource tripleSource, RepositoryConnection conn, BindingSet bindings, FilterValueExpr filterExpr) {
		this.stmt = stmt;
		this.bindings = bindings;
		this.unionControl = unionControl;
		this.tripleSource = tripleSource;
		this.conn = conn;
		this.filterExpr = filterExpr;
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> performTask() throws Exception {
		return tripleSource.getStatements(stmt, conn, bindings, filterExpr);
	}

	@Override
	public ParallelExecutor<BindingSet> getControl() {
		return unionControl;
	}
	
	public String toString() {
		Endpoint e = EndpointManager.getEndpointManager().getEndpoint(conn);
		return this.getClass().getSimpleName() + " @" + e.getId() + ": " + QueryStringUtil.toString(stmt);
	}
}
