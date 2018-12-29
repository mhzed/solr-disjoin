package com.mhzed.solr.disjoin.dv;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

/**
 * For reading NumericDocValues
 * 
 */
public abstract class NumberDocVal<T> extends DocValReader<T>{
	NumericDocValues docvals;
	public NumberDocVal(String field) {
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

  public static NumberDocVal<Integer> intReader(String field) {
    return new NumberDocVal<Integer>(field) {
      @Override
      protected Iterable<Integer> read() throws IOException {
        return Arrays.asList(Long.valueOf(docvals.longValue()).intValue());
      }
    };
  }
  public static NumberDocVal<Double> doubleReader(String field) {
    return new NumberDocVal<Double>(field) {
      @Override
      protected Iterable<Double> read() throws IOException {
        return Arrays.asList(Double.longBitsToDouble(docvals.longValue()));
      }
    };
  }
  public static NumberDocVal<Long> longReader(String field) {
    return new NumberDocVal<Long>(field) {
      @Override
      protected Iterable<Long> read() throws IOException {
        return Arrays.asList(docvals.longValue());
      }
    };
  }
}
