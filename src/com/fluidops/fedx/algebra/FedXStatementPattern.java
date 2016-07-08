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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.StatementPattern;

import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;
import com.fluidops.fedx.util.QueryAlgebraUtil;

/**
 * Base class providing all common functionality for FedX StatementPatterns
 * 
 * @author Andreas Schwarte
 * @see StatementSourcePattern
 * @see ExclusiveStatement
 *
 */
public abstract class FedXStatementPattern extends StatementPattern implements StatementTupleExpr, FilterTuple, BoundJoinTupleExpr
{
	protected final List<StatementSource> statementSources = new ArrayList<StatementSource>();
	protected final int id;
	protected final QueryInfo queryInfo;
	protected final List<String> freeVars = new ArrayList<String>(3);
	protected final List<String> localVars = new ArrayList<String>();
	protected FilterValueExpr filterExpr = null;
	
	public FedXStatementPattern(StatementPattern node, QueryInfo queryInfo) {
		super(node.getSubjectVar(), node.getPredicateVar(), node.getObjectVar(), node.getContextVar());
		this.id = NodeFactory.getNextId();
		this.queryInfo=queryInfo;
		initFreeVars();
	}
	
	protected FedXStatementPattern(Statement st) {
		this(QueryAlgebraUtil.toStatementPattern(st), new QueryInfo("getStatements", QueryType.GET_STATEMENTS));
	}
	
	@Override
	public <X extends Exception> void visitChildren(QueryModelVisitor<X> visitor)
		throws X {
		super.visitChildren(visitor);
		if (localVars.size()>0)
			LocalVarsNode.visit(visitor, localVars);
		for (StatementSource s : sort(statementSources))
			s.visit(visitor);
		
		if (filterExpr!=null)
			filterExpr.visit(visitor);
	}
	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);
	}
	
	protected void initFreeVars() {
		if (getSubjectVar().getValue()==null)
			freeVars.add(getSubjectVar().getName());
		if (getPredicateVar().getValue()==null)
			freeVars.add(getPredicateVar().getName());
		if (getObjectVar().getValue()==null)
			freeVars.add(getObjectVar().getName());
	}

	@Override
	public int getFreeVarCount() {
		return freeVars.size();
	}
	
	@Override
	public List<String> getFreeVars() {
		return freeVars;
	}	
	
	@Override
	public QueryInfo getQueryInfo() {
		return this.queryInfo;
	}

	@Override
	public void addLocalVar(String localVar) {
		this.localVars.add(localVar);		
	}

	@Override
	public List<String> getLocalVars() {
		return localVars;	// TODO
	}
	
	@Override
	public int getId() {
		return id;
	}	

	@Override
	public boolean hasFreeVarsFor(BindingSet bindings) {
		for (String var : freeVars)
			if (!bindings.hasBinding(var))
				return true;
		return false;		
	}
	
	@Override
	public List<StatementSource> getStatementSources() {
		return statementSources;
	}
	
	public int getSourceCount() {
		return statementSources.size();
	}
	
	
	@Override
	public FilterValueExpr getFilterExpr() {
		return filterExpr;
	}

	@Override
	public boolean hasFilter() {
		return filterExpr!=null;
	}

	@Override
	public void addFilterExpr(FilterExpr expr) {

		if (filterExpr==null)
			filterExpr = expr;
		else if (filterExpr instanceof ConjunctiveFilterExpr) {
			((ConjunctiveFilterExpr)filterExpr).addExpression(expr);
		} else if (filterExpr instanceof FilterExpr){
			filterExpr = new ConjunctiveFilterExpr((FilterExpr)filterExpr, expr);
		} else {
			throw new RuntimeException("Unexpected type: " + filterExpr.getClass().getCanonicalName());
		}
	}
	
	@Override
	public void addBoundFilter(String varName, Value value) {
		
		// visit Var nodes and set value for matching var names
		if (getSubjectVar().getName().equals(varName))
			getSubjectVar().setValue(value);
		if (getPredicateVar().getName().equals(varName))
			getPredicateVar().setValue(value);
		if (getObjectVar().getName().equals(varName))
			getObjectVar().setValue(value);
		
		freeVars.remove(varName);
		
		// XXX recheck owned source if it still can deliver results, otherwise prune it
		// optimization: keep result locally for this query
		// if no free vars AND hasResults => replace by TrueNode to avoid additional remote requests
	}
	
	private List<StatementSource> sort(List<StatementSource> stmtSources) {
		List<StatementSource> res = new ArrayList<StatementSource>(stmtSources);
		Collections.sort(res, new Comparator<StatementSource>()	{
			@Override
			public int compare(StatementSource o1, StatementSource o2) 	{
				return o1.id.compareTo(o2.id);
			}			
		});
		return res;
	}
}
