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

package com.fluidops.fedx.cache;

import java.util.List;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Statement;

import com.fluidops.fedx.exception.EntryAlreadyExistsException;
import com.fluidops.fedx.exception.EntryUpdateException;
import com.fluidops.fedx.exception.FedXException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.SubQuery;

/**
 * Interface for any Cache.
 * 
 * @author Andreas Schwarte
 *
 */
public interface Cache {

	public static enum StatementSourceAssurance { NONE, HAS_LOCAL_STATEMENTS, HAS_REMOTE_STATEMENTS, POSSIBLY_HAS_STATEMENTS; }
	
	/**
	 * Ask the cache if a given endpoint can provide results for a subQuery. Note that
	 * S1:={?x, c, c} and S2:={?y, c, c} are treated as the same statement.
	 * 
	 * Subset inference:
	 * 
	 * Cache knows that S1:={?x, c, c} brings results, hence also S2:={?x, ?y, c} will
	 * provide results. This method can be used to test this and will return HAS_STATEMENTS.
	 * 
	 * Superset inference:
	 * 
	 * Cache knows that S1:={?x, ?y, c} brings results, hence S2:={?x, c, c} may provide
	 * results. This method will return POSSIBLY_HAS_STATEMENTS in such a case.
	 * 
	 * @param subQuery
	 * @param endpoint
	 * 
	 * @return
	 *		NONE -> the cache knows that endpoint cannot provide any results
	 *		HAS_LOCAL_STATEMENTS -> the cache has local statements (highest priority)
	 *		HAS_REMOTE_STATEMENTS -> the cache knows that endpoint does provide results
	 *		POSSIBLY_HAS_STATEMENTS -> if the endpoint is not known or in case of superset inference
	 */
	public StatementSourceAssurance canProvideStatements(SubQuery subQuery, Endpoint endpoint);

	
	/**
	 * Ask the cache if it can provide results for a subQuery for any endpoint. Note that
	 * S1:={?x, c, c} and S2:={?y, c, c} are treated as the same statement.
	 * 
	 * Subset inference:
	 * 
	 * Cache knows that S1:={?x, c, c} has local results. However, cache will pessimistically
	 * assume that S2:={?x, ?y, c} has no local results as we cannot assure that the complete
	 * data is available locally. Hence, this method will return false.
	 * 
	 * Superset inference:
	 * 
	 * Cache knows that S1:={?x, ?y, c} has local results, hence S2:={?x, c, c} has local results
	 * as well as the cache maintains a superset of the requested results. In such a case this
	 * method will return true.
	 * 
	 * @param subQuery
	 * @return
	 * 			the endpoints for which local data is available, an empty list otherwise
	 */
	public List<Endpoint> hasLocalStatements(SubQuery subQuery);
	
	
	/**
	 * Ask the cache if it can provide results for a subQuery for the specified endpoint. 
	 * Note that S1:={?x, c, c} and S2:={?y, c, c} are treated as the same statement.
	 * 
	 * Subset inference:
	 * 
	 * Cache knows that S1:={?x, c, c} has local results. However, cache will pessimistically
	 * assume that S2:={?x, ?y, c} has no local results as we cannot assure that the complete
	 * data is available locally. Hence, this method will return false.
	 * 
	 * Superset inference:
	 * 
	 * Cache knows that S1:={?x, ?y, c} has local results, hence S2:={?x, c, c} has local results
	 * as well as the cache maintains a superset of the requested results. In such a case this
	 * method will return true.
	 * 
	 * @param subQuery
	 * @return
	 */
	public boolean hasLocalStatements(SubQuery subQuery, Endpoint endpoint);
	
	
	/**
	 * Retrieve the CacheEntry instance matching the specified subQuery. Note that S1:={?x, c, c} 
	 * and S2:={?y, c, c} are treated as the same statement.
	 * 
	 * @param subQuery
	 * @return
	 * 		the CacheResult or null (if no match was found) - read only clone!
	 */
	public CacheEntry getCacheEntry(SubQuery subQuery);
	
	
	/**
	 * Retrieve an Iterator containing the results for the given subQuery. If no match is
	 * available locally an empty iterator is returned. Note that S1:={?x, c, c} 
	 * and S2:={?y, c, c} are treated as the same statement.
	 * 
	 * @param subQuery
	 * @return
	 * 			an iterator, possible empty if no data is available locally
	 */
	public CloseableIteration<? extends Statement, Exception> getStatements(SubQuery subQuery);
	
	/**
	 * Retrieve an Iterator containing the results for the given subQuery for the given endpoint.
	 * If no match is available locally an empty iterator is returned. Note that S1:={?x, c, c} 
	 * and S2:={?y, c, c} are treated as the same statement.
	 * 
	 * @param subQuery
	 * @return
	 * 			an iterator, possible empty if no data is available locally
	 */
	public CloseableIteration<? extends Statement, Exception> getStatements(SubQuery subQuery, Endpoint endpoint);
	
	
	/**
	 * Initialize this cache, e.g. from file system.
	 * 
	 * @throws Exception
	 */
	public void initialize() throws FedXException;
	
	
	/**
	 * Invalidate some of the contents of this cache, e.g. free not used resources.
	 * 
	 * @throws Exception
	 */
	public void invalidate() throws FedXException;
	
	
	/**
	 * Persist the state of the Cache (optional operation)
	 * 
	 * @throws Exception
	 */
	public void persist() throws FedXException;
	
	
	
	public void addEntry(SubQuery subQuery, CacheEntry cacheEntry) throws EntryAlreadyExistsException;
	
	
	/**
	 * Update the given entry using a merge procedure:
	 * 
	 * Data present in the original is overwritten, if not present it is simply added. Note that data
	 * not touched in the specified merge structure will remain as is.
	 * 
	 * @param subQuery
	 * @param merge
	 * @throws EntryUpdateException
	 */
	public void updateEntry(SubQuery subQuery, CacheEntry merge) throws EntryUpdateException;
	
	
	
	public void removeEntry(SubQuery subQuery) throws EntryUpdateException;
	
	
	public void clear();
}
