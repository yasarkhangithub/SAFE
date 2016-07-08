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

package com.fluidops.fedx.util;

import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;

import com.fluidops.fedx.algebra.ConjunctiveFilterExpr;
import com.fluidops.fedx.algebra.FilterExpr;
import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.exception.FilterConversionException;


/**
 * Various utility functions to handle filter expressions.
 * 
 * NOTE: currently only implemented for {@link Compare}, other filter expressions
 * need to be added. If an unexpected filter expression occurs, the filter 
 * evaluation is done locally.
 * 
 * @author Andreas Schwarte
 */
public class FilterUtils {

	/**
	 * Returns a SPARQL representation of the provided expression,
	 * 
	 * e.g Compare(?v, "<", 3) is converted to "?v < '3'"
	 * 
	 * @param filterExpr
	 * @return
	 * @throws FilterConversionException
	 */
	public static String toSparqlString(FilterValueExpr filterExpr) throws FilterConversionException {
		
		if (filterExpr instanceof FilterExpr)
			return toSparqlString((FilterExpr)filterExpr);
		if (filterExpr instanceof ConjunctiveFilterExpr)
			return toSparqlString((ConjunctiveFilterExpr)filterExpr);
		
		throw new RuntimeException("Unsupported type: " + filterExpr.getClass().getCanonicalName());
	}
	
	
	public static String toSparqlString(FilterExpr filterExpr) throws FilterConversionException {
		StringBuilder sb = new StringBuilder();
		append(filterExpr.getExpression(), sb);
		return sb.toString();
	}
	
	public static String toSparqlString(ConjunctiveFilterExpr filterExpr) throws FilterConversionException {
		
		StringBuilder sb = new StringBuilder();
		int count = 0;
		sb.append("( ");
		for (FilterExpr expr : filterExpr.getExpressions()) {
			append(expr.getExpression(), sb);
			if (++count < filterExpr.getExpressions().size())
				sb.append(" && ");
		}
		sb.append(" )");
		
		return sb.toString();
	}
	
	
	public static ValueExpr toFilter(FilterValueExpr filterExpr) throws FilterConversionException {
		
		if (filterExpr instanceof FilterExpr)
			return toFilter((FilterExpr)filterExpr);
		if (filterExpr instanceof ConjunctiveFilterExpr)
			return toFilter((ConjunctiveFilterExpr)filterExpr);
		
		throw new RuntimeException("Unsupported type: " + filterExpr.getClass().getCanonicalName());
	}
	
	public static ValueExpr toFilter(FilterExpr filterExpr) throws FilterConversionException {
		return filterExpr.getExpression();
	}
	
	public static ValueExpr toFilter(ConjunctiveFilterExpr filterExpr) throws FilterConversionException {
		List<FilterExpr> expressions = filterExpr.getExpressions();
		
		if (expressions.size()==2) {
			return new And(expressions.get(0).getExpression(), expressions.get(0).getExpression());
		}
		
		And and = new And();
		and.setLeftArg( expressions.get(0).getExpression() );
		And tmp = and;
		int idx;
		for (idx=1; idx<expressions.size()-1; idx++) {
			And _a = new And();
			_a.setLeftArg( expressions.get(idx).getExpression() );
			tmp.setRightArg(_a);
			tmp = _a;
		}
		tmp.setRightArg( expressions.get(idx).getExpression());
		
		return and;
	}
	
	protected static void append(ValueExpr expr, StringBuilder sb) throws FilterConversionException {
		
		if (expr instanceof Compare) {
			append((Compare)expr, sb);
		} else if (expr instanceof Var) {
			append((Var)expr, sb);
		} else if (expr instanceof ValueConstant) {
			append((ValueConstant)expr, sb);
		} else {
			// TODO add more!
			throw new FilterConversionException("Expression type not supported, fallback to sesame evaluation: " + expr.getClass().getCanonicalName());
		}
		
	}
	
	protected static void append(Compare cmp, StringBuilder sb) throws FilterConversionException {

		sb.append("( ");
		append(cmp.getLeftArg(), sb);
		sb.append(" ").append(cmp.getOperator().getSymbol()).append(" ");
		append(cmp.getRightArg(), sb);
		sb.append(" )");
	}
	
	
	protected static void append(Var var, StringBuilder sb) {
		if (var.hasValue()) {
			appendValue(sb, var.getValue());
		} else {
			sb.append("?").append(var.getName());
		}
	}
	
	protected static void append(ValueConstant vc, StringBuilder sb) {
		appendValue(sb, vc.getValue());
	}
	
	protected static StringBuilder appendValue(StringBuilder sb, Value value) {

		if (value instanceof URI)
			return appendURI(sb, (URI)value);
		if (value instanceof Literal)
			return appendLiteral(sb, (Literal)value);

		// XXX check for other types ? BNode ?
		throw new RuntimeException("Type not supported: " + value.getClass().getCanonicalName());
	}
	
	
	protected static StringBuilder appendURI(StringBuilder sb, URI uri) {
		sb.append("<").append(uri.stringValue()).append(">");
		return sb;
	}

	protected static StringBuilder appendLiteral(StringBuilder sb, Literal lit) {
		sb.append('\'');
		sb.append(lit.getLabel().replace("\"", "\\\""));
		sb.append('\'');

		if (lit.getLanguage() != null) {
			sb.append('@');
			sb.append(lit.getLanguage());
		}

		if (lit.getDatatype() != null) {
			sb.append("^^<");
			sb.append(lit.getDatatype().stringValue());
			sb.append('>');
		}
		return sb;
	}
}
