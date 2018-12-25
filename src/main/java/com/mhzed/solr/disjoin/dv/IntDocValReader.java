package com.mhzed.solr.disjoin.dv;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * For reading a numeric DocVal as integer.  This applies to solr.IntPointField
 * 
 */
public class IntDocValReader extends DocValReader<Integer>{
	NumericDocValues docvals;
	public IntDocValReader(String field) {
		super(field);
	}
	@Override
	protected boolean attachDocVal(LeafReaderContext context) throws IOException {
		this.docvals = context.reader().getNumericDocValues(field);
		return this.docvals != null;
	}
	@Override
	protected Integer read() throws IOException {
		return Long.valueOf(docvals.longValue()).intValue();
	}
	@Override
	protected int advance(int target) throws IOException {
		return this.docvals == null ? Integer.MAX_VALUE : this.docvals.advance(target);
	}

}
