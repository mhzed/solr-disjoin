package com.mhzed.solr.disjoin.dv;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

/**
 * For reading NumericDocValues
 * 
 */
public abstract class NumbersDocValReader<T> extends DocValReader<T>{
	SortedNumericDocValues docvals;
	public NumbersDocValReader(String field) {
		super(field);
	}
	@Override
	protected boolean attachDocVal(LeafReaderContext context) throws IOException {
		this.docvals = context.reader().getSortedNumericDocValues(field);
		return this.docvals != null;
	}
	@Override
	protected int advance(int target) throws IOException {
		return this.docvals.advance(target);
	}
	@Override
	protected T read() throws IOException {
    for (int i=0; i<docvals.docValueCount(); i++) {
      long v = docvals.nextValue();
    }
    return null;
  }
  
}
