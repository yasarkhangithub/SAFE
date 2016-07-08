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

package com.fluidops.fedx.config;

import java.io.IOException;
import java.util.Collections;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.structures.Endpoint;

/**
 * A {@link SailFactory} that initializes FedX Sails based on 
 * the provided configuration data.
 * 
 * @author Andreas Schwarte
 *
 */
public class FedXSailFactory implements SailFactory {

	/**
	 * The type of repositories that are created by this factory.
	 * 
	 * @see SailFactory#getSailType()
	 */
	public static final String SAIL_TYPE = "fluidops:FedX";
	
	
	@Override
	public SailImplConfig getConfig() {
		return new FedXSailConfig();
	}

	@Override
	public Sail getSail(SailImplConfig config) throws SailConfigException	{
		
		if (!SAIL_TYPE.equals(config.getType())) {
			throw new SailConfigException("Invalid Sail type: " + config.getType());
		}
		
		if (!(config instanceof FedXSailConfig)) {
			throw new SailConfigException("FedXSail config expected, was " + config.getClass().getCanonicalName());
		}	
		
		FedXSailConfig fedXSailConfig = (FedXSailConfig)config;
		String fedxConfig = fedXSailConfig.getFedxConfig();
		
		if (fedxConfig==null)
			throw new SailConfigException("FedX Sail Configuration must not be null");
		
		try	{
			FedXFactory.initializeFederation(fedxConfig, Collections.<Endpoint>emptyList());
		} catch (FedXException e) {
			throw new SailConfigException(e);
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return FederationManager.getInstance().getFederation();
	}

	/**
	 * Returns the Sail's type: <tt>fluidops:FedX</tt>.
	 */
	@Override
	public String getSailType()	{
		return SAIL_TYPE;
	}

}
