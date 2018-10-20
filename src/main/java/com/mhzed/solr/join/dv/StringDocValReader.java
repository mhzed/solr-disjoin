package com.mhzed.solr.join.dv;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;

/**
 * For reading a string DocVal in index during search.  This applies to solr.StrField
 */
public class StringDocValReader extends DocValReader<String>{
	SortedDocValues docvals;
	public StringDocValReader(String field) {
		super(field);
	}
	@Override
	protected boolean attachDocVal(LeafReaderContext context) throws IOException {
		this.docvals = context.reader().getSortedDocValues(field);
		return this.docvals != null;
	}
	@Override
	protected String read() throws IOException {
		return docvals.binaryValue().utf8ToString();
	}
	@Override
	protected int advance(int target) throws IOException {
		return this.docvals == null ? Integer.MAX_VALUE : this.docvals.advance(target);
	}
} 
