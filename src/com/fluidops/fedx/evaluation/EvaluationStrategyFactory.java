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

package com.fluidops.fedx.evaluation;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FederationManager.FederationType;

/**
 * Factory class for retrieving the {@link FederationEvalStrategy} to be used
 * 
 * @author Andreas Schwarte
 */
public class EvaluationStrategyFactory {

	
	/**
	 * Return an instance of {@link FederationEvalStrategy} which is used for evaluating 
	 * the query. The type depends on the {@link FederationType} as well as on the 
	 * actual implementations given by the configuration, in particular this is
	 * {@link Config#getSailEvaluationStrategy()} and {@link Config#getSPARQLEvaluationStrategy()}.
	 * 
	 * @param federationType
	 * @return
	 */
	public static FederationEvalStrategy getEvaluationStrategy(FederationType federationType) {
		
		switch (federationType) {
		case LOCAL:		return instantiate(Config.getConfig().getSailEvaluationStrategy());
		case REMOTE:
		case HYBRID:
		default:		return instantiate(Config.getConfig().getSPARQLEvaluationStrategy());
		}
	}
	
	private static FederationEvalStrategy instantiate(String evalStrategyClass) {
		try {
			return (FederationEvalStrategy)Class.forName(evalStrategyClass).newInstance();
		} catch (InstantiationException e) {
			throw new IllegalStateException("Class " + evalStrategyClass + " could not be instantiated.", e);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Class " + evalStrategyClass + " could not be found, check whether the name is correct.", e);
		} catch (Exception e) {
			throw new IllegalStateException("Unexpected error while instantiating " + evalStrategyClass + ":", e);
		}
	}
}
