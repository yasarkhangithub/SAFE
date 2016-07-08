package com.fluidops.fedx.provider;

import info.aduna.iteration.Iterations;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.structures.Endpoint;

/**
 * Convenience methods for {@link Endpoint} providers
 * 
 * @author Andreas Schwarte
 *
 */
public class ProviderUtil {

	/**
	 * Checks the connection by submitting a SPARQL SELECT query:
	 * 
	 * SELECT * WHERE { ?s ?p ?o } LIMIT 1
	 * 
	 * Throws an exception if the query cannot be evaluated
	 * successfully for some reason (indicating that the 
	 * endpoint is not ok)
	 * 
	 * @param repo
	 * @throws RepositoryException
	 * @throws QueryEvaluationException
	 * @throws MalformedQueryException
	 */
	public static void checkConnectionIfConfigured(Repository repo) throws RepositoryException {
		
		if (!Config.getConfig().isValidateRepositoryConnections())
			return;
		
		RepositoryConnection conn = null;		
		try {
			conn = repo.getConnection();
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1");
			TupleQueryResult qRes = null;
			try {
				qRes = query.evaluate();
				if (!qRes.hasNext())
					throw new RepositoryException("No data in provided repository");
			} finally {
				if (qRes!=null)
					Iterations.closeCloseable(qRes);
			}			
			
		} catch (MalformedQueryException ignore) { 
				;	// can never occur
		} catch (QueryEvaluationException e) {
			throw new RepositoryException(e);
		} finally {			
			if (conn!=null)
				conn.close();
		}
	}
}
