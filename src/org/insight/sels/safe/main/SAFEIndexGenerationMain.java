package org.insight.sels.safe.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.insight.sels.safe.indexgeneration.SAFEendPointIndexGenerator;

/**
 * 
 * @author Yasar Khan, Muhammad Saleem
 *
 */
public class SAFEIndexGenerationMain {

	/**
	 * Main method which executes SAFE index generation
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		// output file i.e SAFE index generated from endpoints specified in "endpoints.txt" file
		String safeIndexFile = "settings" + File.separator + "SAFE-genomics-index.n3";

		// output file
		String endpointsFile = "settings" + File.separator + "endpoints-internal.txt";

		// sparql endpoints for which we need to generate SAFE index
		ArrayList<String> lstEndPoints = SAFEendPointIndexGenerator.getEndpointsList(endpointsFile);

		long startTime = System.currentTimeMillis();

		SAFEendPointIndexGenerator.initializeIndex(safeIndexFile);
		SAFEendPointIndexGenerator.buildIndex(lstEndPoints);
		long endTime = System.currentTimeMillis();

		System.out.println("Index Generation Time (sec): " + (endTime - startTime) / 1000);
		System.out.print("Index is secessfully written to " + safeIndexFile);
	}

}
