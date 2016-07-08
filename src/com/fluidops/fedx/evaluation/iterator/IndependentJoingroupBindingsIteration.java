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

package com.fluidops.fedx.evaluation.iterator;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

/**
 * Inserts original bindings into the result.
 * 
 * @author Andreas Schwarte
 */
public class IndependentJoingroupBindingsIteration extends LookAheadIteration<BindingSet, QueryEvaluationException>{

	protected final BindingSet bindings;
	protected final CloseableIteration<BindingSet, QueryEvaluationException> iter;
	protected ArrayList<BindingSet> result = null;
	protected int currentIdx = 0;
	
	public IndependentJoingroupBindingsIteration(CloseableIteration<BindingSet, QueryEvaluationException> iter, BindingSet bindings) {
		this.bindings = bindings;
		this.iter = iter;
	}

	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		
		if (result==null) {
			result = computeResult();
		}
		
		if (currentIdx>=result.size())
			return null;
		
		return result.get(currentIdx++);
	}

	
	protected ArrayList<BindingSet> computeResult() throws QueryEvaluationException {
		
		List<Binding> a_res = new ArrayList<Binding>();
		List<Binding> b_res = new ArrayList<Binding>();
		
		// collect results XXX later asynchronously
		// assumes that bindingset of iteration has exactly one binding
		while (iter.hasNext()) {
			
			BindingSet bIn = iter.next();
			
			if (bIn.size()!=1)
				throw new RuntimeException("For this optimization a bindingset needs to have exactly one binding, it has " + bIn.size() + ": " + bIn);

			Binding b = bIn.getBinding( bIn.getBindingNames().iterator().next() );
			int bIndex = Integer.parseInt(b.getName().substring(b.getName().lastIndexOf("_")+1));
			
			if (bIndex==0)
				a_res.add(b);
			else if (bIndex==1)
				b_res.add(b);
			else
				throw new RuntimeException("Unexpected binding value.");
		}
		
		ArrayList<BindingSet> res = new ArrayList<BindingSet>(a_res.size() * b_res.size());
		
		for (Binding a : a_res) {
			for (Binding b : b_res) {
				QueryBindingSet newB = new QueryBindingSet(bindings.size() + 2);
				newB.addAll(bindings);
				newB.addBinding(a.getName().substring(0, a.getName().lastIndexOf("_")), a.getValue());
				newB.addBinding(b.getName().substring(0, b.getName().lastIndexOf("_")), b.getValue());
				res.add(newB);
			}
		}
		
		return res;
	}

}
