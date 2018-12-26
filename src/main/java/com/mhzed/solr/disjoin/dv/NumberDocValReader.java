package com.mhzed.solr.disjoin.dv;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * For reading NumericDocValues
 * 
 */
public abstract class NumberDocValReader<T> extends DocValReader<T>{
	NumericDocValues docvals;
	public NumberDocValReader(String field) {
		super(field);
	}
	@Override
	protected boolean attachDocVal(LeafReaderContext context) throws IOException {
		this.docvals = context.reader().getNumericDocValues(field);
		return this.docvals != null;
	}
	@Override
	protected int advance(int target) throws IOException {
		return this.docvals.advance(target);
	}

}
