package com.mhzed.solr.join;

import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;



public final class FilterJoinQParserPlugin extends QParserPlugin {

	public final static String DefaultCacheSpec = "expireAfterAccess=10m,maximumSize=1000";
	private Cache<FilterJoinQuery, Set<Object>> postFilterCache;
	
  @SuppressWarnings("rawtypes")
	@Override
  public void init( NamedList args ) {  	
  	postFilterCache = CacheBuilder.from(NamedListUtil.getOr(args, "cacheSpec", DefaultCacheSpec)).build();
  }
	
	@Override
	public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
						
		return new QParser(qstr, localParams, params, req) {
			@Override
			public Query parse() throws SyntaxError {
				return new FilterJoinQuery(qstr, localParams, params, req, postFilterCache);
			}
		};				
	}	
}

