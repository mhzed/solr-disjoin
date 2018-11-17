package com.mhzed.solr.join.dv;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;

/**
 * For reading DocVals in index after search.  Different field types require calling different methods, which
 * are handled by implementations.  See https://lucene.apache.org/solr/guide/7_4/docvalues.html 
 *
 * @param <T> the type of the walked DocVal
 */
public abstract class DocValReader<T> {
	public final String field;
	protected int id = -1;
	public DocValReader(String field) {
		this.field = field;
	}
	public DocValReader<T> setNullVal(T val) {
		return this;
	}
	public void setDocVal(LeafReaderContext context) throws IOException {
		if (attachDocVal(context)) this.id = -1;
		else this.id = Integer.MAX_VALUE;	// no docval found, return nullVal for every next(...)
	}
	public T next(int docId) throws IOException {
		if (this.id < docId) this.id = advance(docId);
		if (this.id == docId) return read();
		else return this.nullVal();
	}

	// what to return for document that contains no such doc-val field
	protected T nullVal() { return null; }
	protected abstract boolean attachDocVal(LeafReaderContext context) throws IOException;
	protected abstract T read() throws IOException;
	protected abstract int advance(int target) throws IOException;
}

