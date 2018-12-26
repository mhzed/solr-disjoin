package com.mhzed.solr.disjoin.dv;

import java.io.IOException;

/**
 * For reading a numeric DocVal as Long integer.  This applies to 
 * solr.LongPointField 
 * 
 */
public class LongDocValReader extends NumberDocValReader<Long>{
	public LongDocValReader(String field) {
		super(field);
	}
	@Override
	protected Long read() throws IOException {
		return docvals.longValue();
	}
}
