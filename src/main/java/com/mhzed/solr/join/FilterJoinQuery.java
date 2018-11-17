package com.mhzed.solr.join;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;

/**
 * FilterJoinQuery could behave as a normal Query or as a PostFilter.
 * 
 */
public final class FilterJoinQuery extends ExtendedQueryBase implements PostFilter {

	//{!join from=id to=folder_id fromIndex=folders}path:abc
	public static final String ParamFromIndex = "fromIndex";
	public static final String ParamFrom = "from";
	public static final String ParamTo = "to";
	public static final String ParamPost = "pfsz";
	private final static int DefaultPostFilterSize = 10000;
	
	// begin members participating in equals/hashCode
	private String query;
	private String fromField;
	private String toField;
	private String fromIndex;
	// if fromCore is committed, ensure it invalidates this in the cache of current core
	private long fromCoreCommitTime = 0;	
	// end members participating in equals/hashCode
	
	private FieldType toFieldType;
	private int postFilterSize;
	 
	// transient members
	private Supplier<Set<Object>> _joinVals; 	// lazy-eval query on fromIndex.
	private ResponseBuilder _rb;							// for sending back debug info
	
	private boolean postFilter() {
		return _joinVals.get().size() >= this.postFilterSize;
	}
	
	public FilterJoinQuery(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req,
					Cache<FilterJoinQuery, Set<Object>> postFilterCache
					) {
		String v = localParams.get("v");
		query = StringUtils.isEmpty(v) ? qstr : v;
		
		fromField = localParams.get(ParamFrom);
		toField = localParams.get(ParamTo);
		toFieldType = req.getSchema().getField(toField).getType();
		String paramPost = localParams.get(ParamPost);		
		postFilterSize = StringUtils.isEmpty(paramPost) ? DefaultPostFilterSize : Integer.parseInt(paramPost);

		// fromIndex is same as current collection unless specified otherwise
		fromIndex = localParams.get(ParamFromIndex, req.getCore().getCoreDescriptor().getCollectionName());
		SolrCore fromCore = getFromCore(req.getCore());
		
		// fetch last commit time on fromCore
		RefCounted<SolrIndexSearcher> s = fromCore.getRegisteredSearcher();
		if (s != null) {
			fromCoreCommitTime = s.get().getOpenNanoTime();
			s.decref();
		}
 
		// setup lazy-eval
		_joinVals = Suppliers.memoize(()->{
			// post filter is not cached by solr's filter cache, thus cache results in postFilterCache.
			Set<Object> joinVals = postFilterCache.getIfPresent(this);
			if (joinVals == null) {
				joinVals = FilterJoinQuery.this.executeFrom(fromCore);
				if (joinVals.size() >= postFilterSize) postFilterCache.put(this,  joinVals);
			}
			return joinVals;
		});
		
		// get response builder for add debug response
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    if (info != null) {
    	_rb = info.getResponseBuilder();
    }

	}
	
	private SolrCore getFromCore(SolrCore currentCore) {
		return (currentCore.getCoreDescriptor().getCollectionName().equalsIgnoreCase(fromIndex))  ?
						currentCore :
						currentCore.getCoreContainer().getCores().stream().filter(
							(c)->c.getCoreDescriptor().getCollectionName().equalsIgnoreCase(fromIndex)).findFirst().get();
	}

	@Override
	public int getCost() {
		return this.postFilter() ? 100 : 50;
	}
	
	// if to store in filter query cache.  query result cache is un-affected by this value.
	@Override
	public boolean getCache() {
		return this.postFilter() ? false : true;
	}		
	
	@Override
	public boolean equals(Object obj) {
		if (!sameClassAs(obj)) return false;
		FilterJoinQuery other = getClass().cast(obj);
    return this.fromField.equals(other.fromField)
            && this.toField.equals(other.toField)
            && this.query.equals(other.query)
            && fromIndex.equals(other.fromIndex)
            && this.fromCoreCommitTime == other.fromCoreCommitTime
            ;
	}

	@Override
	public int hashCode() {
    return Objects.hash(classHash(), fromField, toField, query, fromIndex, fromCoreCommitTime);
	}
	
		
	@SuppressWarnings("resource")
	private Set<Object> executeFrom(SolrCore fromCore) {
		fromCore.open();	// add ref count
		try {
			return SolrSuspendedQuery.runUnchecked(()->{
				SolrQuery q = new SolrQuery(query).addField(fromField).setRows(Integer.MAX_VALUE);
				return new EmbeddedSolrServer(fromCore).query(q).getResults().stream()
								.map((d)->d.getFieldValue(fromField)).collect(Collectors.toSet());
			});
		} finally {
			fromCore.close(); // dec ref count
		}
	}	
	
	private void buildDebugResponse(boolean postFilter) {
		if (_rb != null && _rb.isDebug()) {
      SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<>();
      dbg.add("type", postFilter ? "post-filter" : "filter");
      dbg.add("fromSetSize", _joinVals.get().size());
      _rb.addDebug(dbg, "FilterJoin", toString());
		}
		this._rb = null;
	}
	
	@Override
	public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
		buildDebugResponse(true);
		DelegatingCollector ret = FilterQueryFactory.createCollecor(toField,  toFieldType,  _joinVals.get());
		this._joinVals = null;
		return ret;
	}
  
  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needScores, float boost) throws IOException {
  	buildDebugResponse(false);
  	Weight ret;
  	Query q = FilterQueryFactory.createSetQuery(toField, toFieldType, _joinVals.get());
    if (!(searcher instanceof SolrIndexSearcher)) {
      // delete-by-query won't have SolrIndexSearcher
    	ret = new BoostQuery(new ConstantScoreQuery(q), 0).createWeight(searcher, needScores, 1f);
    } else {		
	    DocSet docs = ((SolrIndexSearcher)searcher).getDocSet(q);
	    ret = new BoostQuery(new SolrConstantScoreQuery(docs.getTopFilter()), 0).createWeight(searcher, needScores, 1f);
    }
    this._joinVals = null;
    return ret;
  }
	
}
