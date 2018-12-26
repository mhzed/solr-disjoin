package com.mhzed.solr.disjoin.dv;

import java.io.IOException;

/**
 * For reading a numeric DocVal as integer.  This applies to solr.IntPointField
 * 
 */
public class IntDocValReader extends NumberDocValReader<Integer>{
	public IntDocValReader(String field) {
		super(field);
	}
	
	@Override
	protected Integer read() throws IOException {
		return Long.valueOf(docvals.longValue()).intValue();
	}

}
