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

import java.util.List;

import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;

import com.fluidops.fedx.structures.QueryInfo;

/**
 * A tuple expression that represents an nary-Join.
 * 
 * @author Andreas Schwarte
 *
 */
public class NJoin extends NTuple implements TupleExpr {
	
	/**
	 * Construct an nary-tuple. Note that the parentNode of all arguments is
	 * set to this instance.
	 * 
	 * @param args
	 */
	public NJoin(List<TupleExpr> args, QueryInfo queryInfo) {
		super(args, queryInfo);
	}
	
	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);		
	}
	
	
	@Override
	public NJoin clone() {
		return (NJoin)super.clone();
	}	
	
}
