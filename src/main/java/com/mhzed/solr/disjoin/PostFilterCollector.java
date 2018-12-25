package com.mhzed.solr.disjoin;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.solr.search.DelegatingCollector;

import com.mhzed.solr.disjoin.dv.DocValReader;

/**
 *
 * To be used in post filter mode: takes a list of sets, keep document only if the DV(s) are in any of 
 * the set.
 */
public class PostFilterCollector<Reader extends DocValReader<?>> extends DelegatingCollector {
	private final List<Reader> readers;
	private final List<Set<?>> sets;
	public PostFilterCollector(List<Reader> readers, List<Set<?>> sets) {
		this.readers = readers;
    this.sets = sets;
    if (this.readers.size() != this.sets.size() ) throw new IllegalArgumentException();
	}

	@Override
	protected void doSetNextReader(LeafReaderContext context) throws IOException {
    for (Reader r: readers) r.setDocVal(context);
		super.doSetNextReader(context);
	}
	
  @Override
  public void collect(int doc) throws IOException {
    for (int i=0; i<sets.size(); i++) {
      if (sets.get(i).contains(readers.get(i).next(doc))) {
        super.collect(doc);
        break;
      }
    }
  }
}
