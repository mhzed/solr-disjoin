package com.mhzed.solr.join;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.search.DelegatingCollector;

import com.mhzed.solr.join.dv.DocValReader;


/**
 *
 * @param <Reader> DocValReader implementations
 * @param <T> The type of value being filtered
 */
public class PostFilterCollector<Reader extends DocValReader<T>, T> extends DelegatingCollector {
	private final Reader reader;
	private final Set<T> set;
	public PostFilterCollector(Reader reader, Set<T> set) {
		this.reader = reader;
		this.set = set;
	}

	@Override
	protected void doSetNextReader(LeafReaderContext context) throws IOException {
		reader.setDocVal(context);
		super.doSetNextReader(context);
	}
	
  @Override
  public void collect(int doc) throws IOException {
		if (set.contains(reader.next(doc)))
			super.collect(doc);
  }
}
