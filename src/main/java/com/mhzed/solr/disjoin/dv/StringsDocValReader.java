package com.mhzed.solr.disjoin.dv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * For reading a string DocVal in index during search.  This applies to solr.StrField
 */
public class StringsDocValReader extends DocValReader<String>{
	SortedSetDocValues docvals;
	public StringsDocValReader(String field) {
		super(field);
	}
	@Override
	protected boolean attachDocVal(LeafReaderContext context) throws IOException {
		this.docvals = context.reader().getSortedSetDocValues(field);
		return this.docvals != null;
	}
	@Override
	protected Iterable<String> read() throws IOException {
    long ord;
    List<String> r = new ArrayList<String>();
    while ((ord = docvals.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
      final BytesRef term = docvals.lookupOrd(ord);
      r.add(term.utf8ToString());
    }    
    return r;
  }
  
  @Override
	protected int advance(int target) throws IOException {
		return this.docvals.advance(target);
	}
} 
