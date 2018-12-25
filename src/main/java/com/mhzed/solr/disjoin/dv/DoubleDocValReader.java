package com.mhzed.solr.disjoin.dv;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * For reading a numeric DocVal as double.  This applies to solr.FloatPointField, or 
 * solr.DoublePointField 
 * 
 */
public class DoubleDocValReader extends DocValReader<Double>{
	NumericDocValues docvals;
	public DoubleDocValReader(String field) {
		super(field);
	}
	@Override
	protected boolean attachDocVal(LeafReaderContext context) throws IOException {
		this.docvals = context.reader().getNumericDocValues(field);
		return this.docvals != null;
	}
	@Override
	protected Double read() throws IOException {
		return Double.longBitsToDouble(docvals.longValue());
	}
	@Override
	protected int advance(int target) throws IOException {
		return this.docvals == null ? Integer.MAX_VALUE : this.docvals.advance(target);
	}

}
