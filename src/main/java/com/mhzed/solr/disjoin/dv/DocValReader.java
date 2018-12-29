package com.mhzed.solr.disjoin.dv;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.index.LeafReaderContext;

/**
 * For reading DocVals with lucene.Collector.  Different field types require calling different methods, which
 * are handled by implementations.  See https://lucene.apache.org/solr/guide/7_4/docvalues.html 
 *
 * @param <T> the java native type that maps to DocVal
 */
public abstract class DocValReader<T> {
	public final String field;
  protected int id = -1;
	public DocValReader(String field) {
		this.field = field;
	}
	public void setDocVal(LeafReaderContext context) throws IOException {
    if (attachDocVal(context)) this.id = -1;
    else this.id = Integer.MAX_VALUE;	// no docval found, return nullVal for every next(...)
  }
	public Iterable<T> next(int docId) throws IOException {
		if (this.id < docId) this.id = advance(docId);
		if (this.id == docId) return read();
		else return Collections.emptyList();
	}

  // must override
  // returns if context.reader().get*DocValues(field) returned null or NOT.  null is possible if the 
  // entire docset does not contain the docval field.
	protected abstract boolean attachDocVal(LeafReaderContext context) throws IOException;
  protected abstract Iterable<T> read() throws IOException;
  protected abstract int advance(int id) throws IOException;
}

