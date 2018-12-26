package com.mhzed.solr.disjoin.dv;

import java.io.IOException;

/**
 * For reading a numeric DocVal as double.  This applies to solr.FloatPointField, or 
 * solr.DoublePointField 
 * 
 */
public class DoubleDocValReader extends NumberDocValReader<Double>{
	public DoubleDocValReader(String field) {
		super(field);
	}
	@Override
	protected Double read() throws IOException {
		return Double.longBitsToDouble(docvals.longValue());
	}
}
