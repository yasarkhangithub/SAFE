package org.insight.sels.safe.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.jena.query.ResultSetFormatter;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.resultio.text.csv.SPARQLResultsCSVWriter;
import org.openrdf.query.resultio.text.csv.SPARQLResultsCSVWriterFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.monitoring.QueryPlanLog;

public class QueryEvaluation<repo> {

	private static String queryPath = "";
	public static String indexPath = "";
	public static String datasourceConfigPath = "";
	
	public static void main(String[] args) throws Exception {

		if(args.length == 0) {
			System.out.println("[query path] [index path] [datasources config path]");
			System.exit(0);
		}
		queryPath = args[0];
		indexPath = args[1];
//		datasourceConfigPath = args[2];
		
		Config.initialize();
		Config.getConfig().set("enableMonitoring", "true");
		Config.getConfig().set("monitoring.logQueryPlan", "true");
		
//		datasourceConfigPath = "settings/safeDatasourceConfig.ttl";
//		Repository repo = FedXFactory.initializeFederation(datasourceConfigPath);

		
		// External Datasets Endpoints
		SailRepository repo = FedXFactory.initializeSparqlFederation(Arrays.asList(
				"http://datasrv01.deri.ie:8890/sparql", // IMF
				"http://datasrv01.deri.ie:8891/sparql", // WB 
				"http://datasrv01.deri.ie:8892/sparql", // TI
				"http://datasrv01.deri.ie:8893/sparql", // Eurostat
				"http://datasrv01.deri.ie:8894/sparql", // CSO 
				"http://datasrv01.deri.ie:8895/sparql" // FAO
				));
		
		// Internal Datasets Endpoints
//		SailRepository repo = FedXFactory.initializeSparqlFederation(Arrays.asList(
//				"http://localhost:3030/dataset1/sparql",
//				"http://localhost:3030/dataset2/sparql",
//				"http://localhost:3030/dataset3/sparql"
//				));

		String queryString = readFileAsString(queryPath);
		System.out.println(queryString);

		TupleQuery query = repo.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		long startTime = System.currentTimeMillis();
		
//		String outputFilePath = "safe-genome-output.csv";
//		File csvResultFile = new File(outputFilePath);
//		FileOutputStream fos = new FileOutputStream(csvResultFile);
//		
//		TupleQueryResultHandler writer = new SPARQLResultsCSVWriter(fos);	
//		query.evaluate(writer);
		TupleQueryResult res = query.evaluate();
		
		long count = 0;
//		System.out.println(QueryPlanLog.getQueryPlan());
		while (res.hasNext()) {
			res.next();
			// System.out.println(": "+ res.next());
			count++;
		}

		long runTime = System.currentTimeMillis() - startTime;
		System.out.println("Query exection time (msec):" + runTime);
		System.out.println("Total Number of Records: " + count);
		FederationManager.getInstance().shutDown();
		System.out.println("Done.");
		
		System.exit(0);

	}
	
	
	

	/**
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	private static String readFileAsString(String filePath) throws IOException {
		StringBuffer fileData = new StringBuffer();
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		char[] buf = new char[1024];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			String readData = String.valueOf(buf, 0, numRead);
			fileData.append(readData);
		}
		reader.close();
		return fileData.toString();
	}

}
