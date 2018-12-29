package com.mhzed.solr.disjoin.dv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

/**
 * For reading NumericDocValues
 * 
 */
public abstract class NumbersDocVal<T> extends DocValReader<T>{
	SortedNumericDocValues docvals;
	public NumbersDocVal(String field) {
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
  
  public static NumbersDocVal<Long> longReader(String field) {
    return new NumbersDocVal<Long>(field) {
      @Override
      protected Iterable<Long> read() throws IOException {
        List<Long> r = new ArrayList<Long>();
        for (int i=0; i<docvals.docValueCount(); i++) {
          r.add(docvals.nextValue());
        }
        return r;  
      }
    };
  }
  public static NumbersDocVal<Integer> intReader(String field) {
    return new NumbersDocVal<Integer>(field) {
      @Override
      protected Iterable<Integer> read() throws IOException {
        List<Integer> r = new ArrayList<Integer>();
        for (int i=0; i<docvals.docValueCount(); i++) {
          r.add(Long.valueOf(docvals.nextValue()).intValue());
        }
        return r;  
      }
    };
  }
  public static NumbersDocVal<Double> doubleReader(String field) {
    return new NumbersDocVal<Double>(field){
      @Override
      protected Iterable<Double> read() throws IOException {
        List<Double> r = new ArrayList<Double>();
        for (int i=0; i<docvals.docValueCount(); i++) {
          r.add(Double.longBitsToDouble(docvals.nextValue()));
        }
        return r;  
      }
    };
  }
}
