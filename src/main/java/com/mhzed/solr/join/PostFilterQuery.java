package com.mhzed.solr.join;

import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;

/**
 * - A post filter must (see SolrIndexSearcher:1023)
 * 		1. implements PostFilter
 * 		2. getCache() return false
 *    3. cost >= 100
 * 
 *
 */
public final class PostFilterQuery extends ExtendedQueryBase implements PostFilter {
	private DelegatingCollector filter;
	
	public PostFilterQuery(DelegatingCollector filter) {
		this.filter = filter;
		this.setCost(100);
	}

	@Override
	public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
		return filter;
	}
	
	@Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    throw new UnsupportedOperationException("Query !!!! does not implement createWeight");
  }
	@Override
	public boolean getCache() {
		return false;
	}		
	@Override
	public boolean equals(Object other) {
		return false; 
	}
	@Override
	public int hashCode() {
		return classHash();
	}
}	