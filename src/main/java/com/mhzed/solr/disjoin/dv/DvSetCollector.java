package com.mhzed.solr.disjoin.dv;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

/**
 * For collecting docvals into a set.
 */
public class DvSetCollector<T> extends SimpleCollector {
  final DocValReader<T> dvr ;
  final Set<T> results;
  
  public DvSetCollector(DocValReader<T> dvr, Set<T> results) {
    this.dvr = dvr;
    this.results = results;
  }

  @Override
  public boolean needsScores() {
    return false;
  }
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    dvr.setDocVal(context);
  }
  @Override
  public void collect(int doc) throws IOException {
    results.add(dvr.next(doc));
  }

}