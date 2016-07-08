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

package com.fluidops.fedx.structures;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

public class UnboundStatement implements Statement {

	private static final long serialVersionUID = 2612189412333330052L;

	
	protected final Resource subj;
	protected final URI pred;
	protected final Value obj;
	
		
	public UnboundStatement(Resource subj, URI pred, Value obj) {
		super();
		this.subj = subj;
		this.pred = pred;
		this.obj = obj;
	}

	@Override
	public Resource getContext() {
		return null;
	}

	@Override
	public Value getObject() {
		return obj;
	}

	@Override
	public URI getPredicate() {
		return pred;
	}

	@Override
	public Resource getSubject() {
		return subj;
	}

}
