package com.mhzed.solr.disjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.mhzed.solr.disjoin.dv.DocValReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

/**
 * DisJoinQuery could behave as a normal Query or as a PostFilter, depending
 * on the size of join.
 * If in PostFilter mode, then the join set values are cached in postFilterCache
 * configured in DisJoinQParserPlugin.  Otherwise, postFilterCache is not used since
 * the target collection's filter cache is used and is more effective.
 * By default the PostFilter mode kicks in when the join set size exceeds 10000, this value
 * can be overridden by query local parameter 'pfsz'.
 *
 * To pass in multiple queries, use additional parameters v1, v2, etc.... 
 * For example:
 *      {!djoin v=fromindex.id|folder_id|path:abc v1=id2|id2|content:blah ...} 
 * 
 */
public final class DisJoinQuery extends ExtendedQueryBase implements PostFilter {
  public static final String ParamPost = "pfsz";
  public static final int DefaultPostFilterSize = 10000;

  private List<JoinQstr> joinQueries;
  private Supplier<List<Set<?>>> joinVals;

  // if any of the join set size exceeds postFilterSize, DisJoinQuery will run in
  // post filter mode.
  private int postFilterSize;
  private ResponseBuilder _rb; // for sending back debug info

  private int size(List<Set<?>> s) {
    return s.stream().reduce(0, (a,b)->a+b.size(), (a,b)->a+b);
  }
	private boolean isPostFilter() {
    return joinVals == null ? false : size(joinVals.get()) >= this.postFilterSize;
	}
  
  /**
   * postFilterCache: is used to cache query results fetched from fromIndex
   */
	public DisJoinQuery(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req,
					Cache<DisJoinQuery, List<Set<?>>> postFilterCache, QParser qParser
					) throws SyntaxError {
    this.joinQueries = new ArrayList<JoinQstr>();
    for (int i=0; true; i++) {
      String query;
      if (i==0) {
        if (localParams.get("v") != null) query = localParams.get("v");
        else query = qstr;
      } else {    // v1, v2, v3, ...
        query = localParams.get("v" + i);
      }
      if (query == null) break;
      this.joinQueries.add(JoinQstr.create(query, req, qParser));
    }
    
		String paramPost = localParams.get(ParamPost);		
		postFilterSize = StringUtils.isEmpty(paramPost) ? DefaultPostFilterSize : Integer.parseInt(paramPost);
 
		// setup lazy-eval
		joinVals = Suppliers.memoize(()->{
			// post filter is not cached by solr's filter cache, thus cache results in postFilterCache.
			List<Set<?>> joinVals = postFilterCache.getIfPresent(this);
			if (joinVals == null) {
        joinVals = this.joinQueries.stream().map(q->q.exec()).collect(Collectors.toList());
				if (size(joinVals) >= postFilterSize) postFilterCache.put(this,  joinVals);
			}
			return joinVals;
		});
		
		// get response builder for add debug response
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    if (info != null) {
    	_rb = info.getResponseBuilder();
    }

	}
	
	@Override
	public int getCost() {
		return this.isPostFilter() ? 100 : 50;
	}
	
	// if to store in filter query cache.  query result cache is un-affected by this value.
	@Override
	public boolean getCache() {
		return this.isPostFilter() ? false : true;
	}		
	
	@Override
	public boolean equals(Object obj) {
		if (!sameClassAs(obj)) return false;
		DisJoinQuery other = getClass().cast(obj);
    return this.joinQueries.equals(other.joinQueries);
	}

	@Override
	public int hashCode() {
    return Objects.hash(classHash(), this.joinQueries);
	}
		
	private void buildDebugResponse(boolean postFilter) {
		if (_rb != null && _rb.isDebug()) {
      SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<>();
      dbg.add("type", postFilter ? "post-filter" : "filter");
      dbg.add("fromSetSize",size(joinVals.get()));
      _rb.addDebug(dbg, "FilterJoin", toString());
		}
		this._rb = null;
	}
  
  // called when run as postFilter
	@Override
	public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    buildDebugResponse(true);

    List<DocValReader<?>> readers = new ArrayList<DocValReader<?>>(); 
    List<Set<?>> sets = new ArrayList<Set<?>>();
    for (int i=0; i<this.joinQueries.size(); i++) {
      JoinQstr jq = this.joinQueries.get(i);
      Set<?> set = this.joinVals.get().get(i);
      DisJoinQueryUtil.CompatibleDataType t = DisJoinQueryUtil.parseType(jq.toFieldType);
      if (set.size() > 0) {
        DisJoinQueryUtil.typeCheck(set.iterator().next(), t);  // ensure type compatibility
      }
      for (int j=0; j<jq.toFields.size(); j++) {
        readers.add(DisJoinQueryUtil.getDocValReader(jq.toFields.get(j), t));
        sets.add(set);
      }
    }
		DelegatingCollector ret = new PostFilterCollector<DocValReader<?>>(readers, sets);
		this.joinVals = null;
		return ret;
	}
  
  // called when run as normal query
  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needScores, float boost) throws IOException {
  	buildDebugResponse(false);

    BooleanQuery.Builder b = new BooleanQuery.Builder();
    for (int i=0; i<this.joinQueries.size(); i++) {
      JoinQstr jq = this.joinQueries.get(i);
      Set<?> set = this.joinVals.get().get(i);
      for (int j=0; j<jq.toFields.size(); j++) {
        b.add(DisJoinQueryUtil.createSetQuery(jq.toFields.get(j), jq.toFieldType, set), Occur.SHOULD);
      }
    }
    Query q = b.build();

    Weight ret;
    if (!(searcher instanceof SolrIndexSearcher)) {
      // delete-by-query won't have SolrIndexSearcher
    	ret = new ConstantScoreQuery(q).createWeight(searcher, needScores, 1f);
    } else {		
	    DocSet docs = ((SolrIndexSearcher)searcher).getDocSet(q);
	    ret = new SolrConstantScoreQuery(docs.getTopFilter()).createWeight(searcher, needScores, 1f);
    }
    this.joinVals = null;
    return ret;
  }
	
}
