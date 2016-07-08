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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.fluidops.fedx.algebra.EmptyNJoin;
import com.fluidops.fedx.algebra.EmptyResult;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.NJoin;
import com.fluidops.fedx.algebra.TrueStatementPattern;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.util.QueryStringUtil;


/**
 * Optimizer with the following tasks:
 * 
 * 1. Group {@link ExclusiveStatement} into {@link ExclusiveGroup}
 * 2. Adjust the join order using {@link JoinOrderOptimizer}
 * 
 * 
 * @author as
 */
public class StatementGroupOptimizer extends QueryModelVisitorBase<OptimizationException> implements FedXOptimizer {

	public static Logger log = Logger.getLogger(StatementGroupOptimizer.class);
	
	protected final QueryInfo queryInfo;
		
	public StatementGroupOptimizer(QueryInfo queryInfo) {
		super();
		this.queryInfo = queryInfo;
	}



	@Override
	public void optimize(TupleExpr tupleExpr) {
		tupleExpr.visit(this);
	}

	@Override
	public void meet(Service tupleExpr) {
		// stop traversal
	}
	
	
	@Override
	public void meetOther(QueryModelNode node) {
		if (node instanceof NJoin) {
			super.meetOther(node);		// depth first
			meetNJoin((NJoin) node);
		} else {
			super.meetOther(node);
		}
	}

	
	protected void meetNJoin(NJoin node) {
		
		LinkedList<TupleExpr> newArgs = new LinkedList<TupleExpr>();
		
		LinkedList<TupleExpr> argsCopy = new LinkedList<TupleExpr>(node.getArgs());
		while (!argsCopy.isEmpty()) {
			
			TupleExpr t = argsCopy.removeFirst();
			
			/*
			 * If one of the join arguments cannot produce results,
			 * the whole join expression does not produce results.
			 * => replace with empty join and return
			 */
			if (t instanceof EmptyResult) {				
				node.replaceWith( new EmptyNJoin(node, queryInfo));
				return;
			}
			
			/*
			 * for exclusive statements find those belonging to the 
			 * same source (if any) and form exclusive group
			 */
			else if (t instanceof ExclusiveStatement) {
				ExclusiveStatement current = (ExclusiveStatement)t;
				
				List<ExclusiveStatement> l = null;
				for (TupleExpr te : argsCopy) {		
					/* in the remaining join args find exclusive statements
					 * having the same source, and add to a list which is
					 * later used to form an exclusive group
					 */
					if (te instanceof ExclusiveStatement) {
						ExclusiveStatement check = (ExclusiveStatement)te;
						if (check.getOwner().equals(current.getOwner())) {
							if (l==null) {
								l = new ArrayList<ExclusiveStatement>();
								l.add(current);
							}
							l.add(check);
						}							
					}						
				}
				
				
				// check if we can construct a group, otherwise add directly
				if (l!=null) {
					argsCopy.removeAll(l);
					newArgs.add( new ExclusiveGroup(l, current.getOwner(), queryInfo ));
				} else {
					newArgs.add( current );
				}
			}
			
			/*
			 * statement yields true in any case, not needed for join
			 */
			else if (t instanceof TrueStatementPattern) {
				if (log.isDebugEnabled())
					log.debug("Statement " + QueryStringUtil.toString((StatementPattern)t) + " yields results for at least one provided source, prune it.");
			}
			
			else
				newArgs.add(t);
		}
		
		// if the join args could be reduced to just one, e.g. OwnedGroup
		// we can safely replace the join node
		if (newArgs.size()==1) {
			log.debug("Join arguments could be reduced to a single argument, replacing join node.");
			node.replaceWith( newArgs.get(0) );
			return;
		}
		
		// in rare cases the join args can be reduced to 0, e.g. if all statements are 
		// TrueStatementPatterns. We can safely replace the join node in such case
		if (newArgs.size()==0) {
			log.debug("Join could be pruned as all join statements evaluate to true, replacing join with true node.");
			node.replaceWith( new TrueStatementPattern( new StatementPattern()));
			return;
		}
		
		List<TupleExpr> optimized = newArgs;
		
		// optimize the join order
		optimized = JoinOrderOptimizer.optimizeJoinOrder(optimized);

		// exchange the node
		NJoin newNode = new NJoin(optimized, queryInfo);
		node.replaceWith(newNode);
	}	
	
}
