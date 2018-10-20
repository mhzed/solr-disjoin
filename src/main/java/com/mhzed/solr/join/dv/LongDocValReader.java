package com.mhzed.solr.join.dv;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * For reading a numeric DocVal as Long integer.  This applies to solr.IntPointField, or 
 * solr.LongPointField 
 * 
 */
public class LongDocValReader extends DocValReader<Long>{
	NumericDocValues docvals;
	public LongDocValReader(String field) {
		super(field);
	}
	@Override
	protected boolean attachDocVal(LeafReaderContext context) throws IOException {
		this.docvals = context.reader().getNumericDocValues(field);
		return this.docvals != null;
	}
	@Override
	protected Long read() throws IOException {
		return docvals.longValue();
	}
	@Override
	protected int advance(int target) throws IOException {
		return this.docvals == null ? Integer.MAX_VALUE : this.docvals.advance(target);
	}

}
