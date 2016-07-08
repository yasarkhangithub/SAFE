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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.impl.EmptyBindingSet;

import com.fluidops.fedx.algebra.FilterValueExpr;
import com.fluidops.fedx.algebra.IndependentJoinGroup;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.ExclusiveGroup;
import com.fluidops.fedx.exception.FilterConversionException;
import com.fluidops.fedx.exception.IllegalQueryException;

/**
 * Various static functions for query handling and parsing (alegbra expression).
 * 
 * @author Andreas Schwarte
 */
public class QueryAlgebraUtil {

	public static Logger log = Logger.getLogger(QueryAlgebraUtil.class);
	
	/**
	 * returns true iff there is at least one free variable, i.e. there is no binding
	 * for any variable
	 * 
	 * @param stmt
	 * @param bindings
	 * @return
	 */
	public static boolean hasFreeVars(StatementPattern stmt, BindingSet bindings) {
		for (Var var : stmt.getVarList()) {
			if(!var.hasValue() && !bindings.hasBinding(var.getName()))
				return true;	// there is at least one free var				
		}
		return false;
	}
	
	/**
	 * Return the {@link Value} of the variable which is either taken from
	 * the variable itself (bound) or from the bindingsset (unbound).
	 * 
	 * @param var
	 * @param bindings
	 * 			the bindings, must not be null, use {@link EmptyBindingSet} instead
	 * 
	 * @return
	 * 		the value or null
	 */
	public static Value getVarValue(Var var, BindingSet bindings) {
		if (var == null) {
			return null;
		} else if (var.hasValue()) {
			return var.getValue();
		} else {
			return bindings.getValue(var.getName());
		}
	}
		
	
	public static StatementPattern toStatementPattern(Statement stmt) {
		return toStatementPattern(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
	}
	
	public static StatementPattern toStatementPattern(Resource subj, URI pred, Value obj) {
		Var s = subj==null ? new Var("s") : new Var("const_s", subj);
		Var p = pred==null ? new Var("p") : new Var("const_p", pred);
		Var o = obj==null ? new Var("o") : new Var("const_o", obj);
		// TODO context
		
		return new StatementPattern(s, p, o);
	}
	
	public static Statement toStatement(StatementPattern stmt) {
		return toStatement(stmt, EmptyBindingSet.getInstance());
	}
	
	public static Statement toStatement(StatementPattern stmt, BindingSet bindings) {
		
		Value subj = getVarValue(stmt.getSubjectVar(), bindings);
		Value pred = getVarValue(stmt.getPredicateVar(), bindings);
		Value obj = getVarValue(stmt.getObjectVar(), bindings);
		// TODO context
		
		return new StatementImpl((Resource)subj, (URI)pred, obj);
	}
	
	
	/**
	 * Construct a SELECT query for the provided statement. 
	 * 
	 * @param stmt
	 * @param bindings
	 * @param filterExpr
	 * @param evaluated
	 * 			parameter can be used outside this method to check whether FILTER has been evaluated, false in beginning
	 * 
	 * @return
	 * @throws IllegalQueryException
	 */
	public static TupleExpr selectQuery( StatementPattern stmt, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated) throws IllegalQueryException {
		
		Set<String> varNames = new HashSet<String>();
		TupleExpr expr = constructStatement(stmt, varNames, bindings);
				
		if (varNames.size()==0)
			throw new IllegalQueryException("SELECT query needs at least one projection!");
				
		if (filterExpr!=null) {
			try {
				expr = new Filter(expr, FilterUtils.toFilter(filterExpr));
				evaluated = true;
			} catch (FilterConversionException e) {
				log.warn("Filter could not be evaluated remotely. " + e.getMessage());
			}
		}
	
		ProjectionElemList projList = new ProjectionElemList();
		for (String var : varNames)
			projList.addElement( new ProjectionElem(var));
		
		Projection proj = new Projection(expr, projList);
		
		return proj;	
	}
	
	/**
	 * Construct a SELECT query for the provided {@link ExclusiveGroup}. Note that bindings
	 * and filterExpr are applied whenever possible.
	 *  
	 * @param group
	 * 				the expression for the query
	 * @param bindings
	 * 				the bindings to be applied
	 * @param filterExpr
	 * 				a filter expression or null
	 * @param evaluated
	 * 				parameter can be used outside this method to check whether FILTER has been evaluated, false in beginning
	 * 
	 * @return
	 */
	public static TupleExpr selectQuery( ExclusiveGroup group, BindingSet bindings, FilterValueExpr filterExpr, Boolean evaluated) {
		
		
		Set<String> varNames = new HashSet<String>();
		List<ExclusiveStatement> stmts = group.getStatements();
		
		Join join = null;
		
		if (stmts.size()==2) {
			join = new Join(constructStatement(stmts.get(0), varNames, bindings), constructStatement(stmts.get(1), varNames, bindings));
		} else {
			join = new Join();
			join.setLeftArg(constructStatement(stmts.get(0), varNames, bindings) );
			Join tmp = join;
			int idx;
			for (idx=1; idx<stmts.size()-1; idx++) {
				Join _u = new Join();
				_u.setLeftArg( constructStatement(stmts.get(idx), varNames, bindings) );
				tmp.setRightArg(_u);
				tmp = _u;
			}
			tmp.setRightArg( constructStatement(stmts.get(idx), varNames, bindings) );
		}	
		
		TupleExpr expr = join;
				
		if (filterExpr!=null) {
			try {
				expr = new Filter(expr, FilterUtils.toFilter(filterExpr));
				evaluated = true;
			} catch (FilterConversionException e) {
				log.warn("Filter could not be evaluated remotely. " + e.getMessage());
			}
		}
		
		ProjectionElemList projList = new ProjectionElemList();
		for (String var : varNames)
			projList.addElement( new ProjectionElem(var));
		
		Projection proj = new Projection(expr, projList);
		
		return proj;
	}

	/**
	 * Construct a SELECT query expression for a bound union.
	 * 
	 * Pattern:
	 * 
	 * SELECT ?v_1 ?v_2 ?v_N WHERE { { ?v_1 p o } UNION { ?v_2 p o } UNION ... } 
	 * 
	 * Note that the filterExpr is not evaluated at the moment.
	 * 
	 * @param stmt
	 * @param unionBindings
	 * @param filterExpr
	 * @param evaluated
	 * 			parameter can be used outside this method to check whether FILTER has been evaluated, false in beginning
	 * 
	 * @return
	 */
	public static TupleExpr selectQueryBoundUnion( StatementPattern stmt, List<BindingSet> unionBindings, FilterValueExpr filterExpr, Boolean evaluated) {
		
		// TODO add FILTER expressions
		
		Set<String> varNames = new HashSet<String>();
		
		Union union = new Union();
		union.setLeftArg(constructStatementId(stmt, Integer.toString(0), varNames, unionBindings.get(0)) );
		Union tmp = union;
		int idx;
		for (idx=1; idx<unionBindings.size()-1; idx++) {
			Union _u = new Union();
			_u.setLeftArg( constructStatementId(stmt, Integer.toString(idx), varNames, unionBindings.get(idx)) );
			tmp.setRightArg(_u);
			tmp = _u;
		}
		tmp.setRightArg( constructStatementId(stmt, Integer.toString(idx), varNames, unionBindings.get(idx) ));
				
		ProjectionElemList projList = new ProjectionElemList();
		for (String var : varNames)
			projList.addElement( new ProjectionElem(var));
		
		Projection proj = new Projection(union, projList);

		return proj;
	}	
		
	
	/**
	 * Construct a SELECT query for a grouped bound check.
	 * 
	 * Pattern:
	 * 
	 * SELECT DISTINCT ?o_1 .. ?o_N WHERE { { s1 p1 ?o_1 FILTER ?o_1=o1 } UNION ... UNION { sN pN ?o_N FILTER ?o_N=oN }}
	 * 
	 * @param stmt
	 * @param unionBindings
	 * @return
	 */
	public static TupleExpr selectQueryStringBoundCheck(StatementPattern stmt, List<BindingSet> unionBindings) {
		
		Set<String> varNames = new HashSet<String>();
		
		Union union = new Union();
		union.setLeftArg(constructStatementCheckId(stmt, 0, varNames, unionBindings.get(0)) );
		Union tmp = union;
		int idx;
		for (idx=1; idx<unionBindings.size()-1; idx++) {
			Union _u = new Union();
			_u.setLeftArg( constructStatementCheckId(stmt, idx, varNames, unionBindings.get(idx)) );
			tmp.setRightArg(_u);
			tmp = _u;
		}
		tmp.setRightArg( constructStatementCheckId(stmt, idx, varNames, unionBindings.get(idx) ));
		
		ProjectionElemList projList = new ProjectionElemList();
		for (String var : varNames)
			projList.addElement( new ProjectionElem(var));
		
		Projection proj = new Projection(union, projList);

		return proj;
	}	
	
	
	public static TupleExpr selectQueryIndependentJoinGroup(IndependentJoinGroup joinGroup, BindingSet bindings) {
		
		Set<String> varNames = new HashSet<String>();
		
		
		Union union = null;
		
		if (joinGroup.getMemberCount()==2) {
			union = new Union();
			union.setLeftArg(constructStatementId((StatementPattern)joinGroup.getMembers().get(0), Integer.toString(0), varNames, bindings));
			union.setRightArg(constructStatementId((StatementPattern)joinGroup.getMembers().get(1), Integer.toString(1), varNames, bindings));
		} else {
			union = new Union();
			union.setLeftArg(constructStatementId((StatementPattern)joinGroup.getMembers().get(0), Integer.toString(0), varNames, bindings) );
			Union tmp = union;
			int idx;
			for (idx=1; idx<joinGroup.getMemberCount()-1; idx++) {
				Union _u = new Union();
				_u.setLeftArg( constructStatementId((StatementPattern)joinGroup.getMembers().get(idx), Integer.toString(idx), varNames, bindings) );
				tmp.setRightArg(_u);
				tmp = _u;
			}
			tmp.setRightArg( constructStatementId((StatementPattern)joinGroup.getMembers().get(idx), Integer.toString(idx), varNames, bindings));
		}		
		
		ProjectionElemList projList = new ProjectionElemList();
		for (String var : varNames)
			projList.addElement( new ProjectionElem(var));
		
		Projection proj = new Projection(union, projList);

		return proj;
	}
	
	
	/**
	 * Construct a select query representing a bound independent join group.
	 * 
	 * ?v_%stmt%#%bindingId$
	 * 
	 * SELECT ?v_0_0 ?v_0_1 ?v_1_0 ... 
	 * 	WHERE { 
	 * 		{ ?v_0#0 p o UNION ?v_0_1 p o UNION ... } 
	 * 		UNION 
	 * 		{ ?v_1_0 p o UNION ?v_1_1 p o UNION ... } 
	 *      UNION
	 *      ...
	 *  }
	 * @param joinGroup
	 * @param bindings
	 * @return
	 */
	public static TupleExpr selectQueryIndependentJoinGroup(IndependentJoinGroup joinGroup, List<BindingSet> bindings) {
		
		Set<String> varNames = new HashSet<String>();
		
		
		Union outer = null;
		
		if (joinGroup.getMemberCount()==2) {
			outer = new Union();
			outer.setLeftArg(constructInnerUnion((StatementPattern)joinGroup.getMembers().get(0), 0, varNames, bindings));
			outer.setRightArg(constructInnerUnion((StatementPattern)joinGroup.getMembers().get(1), 1, varNames, bindings));
		} else {			
			throw new RuntimeException("TODOO");
		}		
		
		ProjectionElemList projList = new ProjectionElemList();
		for (String var : varNames)
			projList.addElement( new ProjectionElem(var));
		
		Projection proj = new Projection(outer, projList);

		return proj;
	}
	
	
	protected static Union constructInnerUnion(StatementPattern stmt, int outerID, Set<String> varNames, List<BindingSet> bindings) {
		
		Union union = new Union();
		union.setLeftArg(constructStatementId(stmt, outerID + "_0", varNames, bindings.get(0)) );
		Union tmp = union;
		int idx;
		for (idx=1; idx<bindings.size()-1; idx++) {
			Union _u = new Union();
			_u.setLeftArg( constructStatementId(stmt, outerID + "_" + idx, varNames, bindings.get(idx)) );
			tmp.setRightArg(_u);
			tmp = _u;
		}
		tmp.setRightArg( constructStatementId(stmt, outerID + "_" + idx, varNames, bindings.get(idx)));
		
		return union;
	}
	

	/**
	 * Construct the statement string, i.e. "s p o . " with bindings inserted wherever possible. Note that
	 * the free variables are added to the varNames set for further evaluation.
	 * 
	 * @param stmt
	 * @param varNames
	 * @param bindings
	 * 
	 * @return
	 */
	protected static StatementPattern constructStatement(StatementPattern stmt, Set<String> varNames, BindingSet bindings) {
		
		Var subj = appendVar(stmt.getSubjectVar(), varNames, bindings);
		Var pred =  appendVar(stmt.getPredicateVar(), varNames, bindings);
		Var obj = appendVar(stmt.getObjectVar(), varNames, bindings);
		
		return new StatementPattern(subj, pred, obj);
	}
	
	/**
	 * Construct the statement string, i.e. "s p o . " with bindings inserted wherever possible. Variables
	 * are renamed to "var_"+varId to identify query results in bound queries. Note that
	 * the free variables are also added to the varNames set for further evaluation.
	 * 
	 * @param stmt
	 * @param varNames
	 * @param bindings
	 * 
	 * @return
	 */
	protected static StatementPattern constructStatementId(StatementPattern stmt, String varID, Set<String> varNames, BindingSet bindings) {
		
		
		Var subj = appendVarId(stmt.getSubjectVar(), varID, varNames, bindings);
		Var pred =  appendVarId(stmt.getPredicateVar(), varID, varNames, bindings);
		Var obj = appendVarId(stmt.getObjectVar(), varID, varNames, bindings);
		
		return new StatementPattern(subj, pred, obj);
	}
	
	/**
	 * Construct the statement string, i.e. "s p ?o_varID FILTER ?o_N=o ". This kind of statement
	 * pattern is necessary to later on identify available results.
	 * 
	 * @param stmt
	 * @param varID
	 * @param varNames
	 * @param bindings
	 * @return
	 */
	protected static TupleExpr constructStatementCheckId(StatementPattern stmt, int varID, Set<String> varNames, BindingSet bindings) {
		
		String _varID = Integer.toString(varID);
		Var subj = appendVarId(stmt.getSubjectVar(), _varID, varNames, bindings);
		Var pred = appendVarId(stmt.getPredicateVar(), _varID, varNames, bindings);
		
		Var obj = new Var("o_" + _varID);
		varNames.add("o_" + _varID);
				
		Value objValue;
		if (stmt.getObjectVar().hasValue()) {
			objValue = stmt.getObjectVar().getValue();
		} else if (bindings.hasBinding(stmt.getObjectVar().getName())){
			objValue = bindings.getBinding(stmt.getObjectVar().getName()).getValue();
		} else {
			// just to make sure that we see an error, will be deleted soon
			throw new RuntimeException("Unexpected.");
		}
		
		Compare cmp = new Compare(obj, new ValueConstant(objValue));
		cmp.setOperator(CompareOp.EQ);
		Filter filter = new Filter( new StatementPattern(subj, pred, obj), cmp);
				
		return filter;
	}
	
	
	
	
	/**
	 * Clone the specified variable and attach bindings.
	 *  
	 * @param sb
	 * @param var
	 * @param varNames
	 * @param bindings
	 * 
	 * @return

	 */
	protected static Var appendVar(Var var, Set<String> varNames, BindingSet bindings) {
		Var res = var.clone();
		if (!var.hasValue()) {
			if (bindings.hasBinding(var.getName()))
				res.setValue( bindings.getValue(var.getName()) );
			else 
				varNames.add(var.getName());			
		}
		return res;
	}
	
	/**
	 * Clone the specified variable and attach bindings, moreover change name of variable
	 * by appending "_varId" to it.
	 * 
	 * @param sb
	 * @param var
	 * @param varNames
	 * @param bindings
	 * 
	 * @return
	 */
	protected static Var appendVarId(Var var, String varID, Set<String> varNames, BindingSet bindings) {
		Var res = var.clone();
		if (!var.hasValue()) {
			if (bindings.hasBinding(var.getName())) {
				res.setValue( bindings.getValue(var.getName()) );
			} else {
				String newName = var.getName() + "_" + varID;
				varNames.add(newName);
				res.setName(newName);
			}			
		}
		return res;
	}
		
	
	
}
