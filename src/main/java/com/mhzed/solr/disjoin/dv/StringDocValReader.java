package com.mhzed.solr.disjoin.dv;

import java.io.IOException;
import java.util.Collections;

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
	protected Iterable<String> read() throws IOException {
		return Collections.singletonList(docvals.binaryValue().utf8ToString());
	}
	@Override
	protected int advance(int target) throws IOException {
		return this.docvals.advance(target);
	}
} 
