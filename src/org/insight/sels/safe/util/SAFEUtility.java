package org.insight.sels.safe.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.JOptionPane;

import com.fluidops.fedx.structures.QueryInfo;


public class SAFEUtility {
	
	public static boolean flag = true;
	
	/**
	 * 
	 * @param namedGraphMap
	 * @return
	 */
	public static String addNamedGraphs(HashMap<Integer, String> namedGraphMap) {
		
		String namedGraphs = "";
		
		Iterator graphIter = namedGraphMap.entrySet().iterator();
	    while (graphIter.hasNext()) {
	        Map.Entry pair = (Map.Entry)graphIter.next();
	        Integer key = (Integer) pair.getKey();
	        String value = (String) pair.getValue();
//	        System.out.println(key + " = " + value);
	        namedGraphs += "FROM NAMED <" + value + ">"
					+ System.getProperty("line.separator");
	    }

		return namedGraphs;
	}
	
	
	public static String addNamedGraphsToSingleSrcQuery(HashMap<Integer, String> namedGraphMap, String query) {
		
		String modifiedQuery = "";
		
		if(flag) {
			String namedGraphString = SAFEUtility.addNamedGraphs(namedGraphMap);
			modifiedQuery = query.replace("where", "");
			modifiedQuery = modifiedQuery.replace("WHERE", "");
			modifiedQuery = modifiedQuery.replaceFirst("\\{", "\n" + namedGraphString + " WHERE { Graph ?g {");
			int ind = modifiedQuery.lastIndexOf("}");
			if(ind >= 0)
				modifiedQuery = new StringBuilder(modifiedQuery).replace(ind, ind+1, "} }").toString();
		} else
			modifiedQuery = query;
		
		flag = false;
				
//		System.out.println("Modified Query:  " + modifiedQuery);
		
		return modifiedQuery;
		
	}

}
