package com.mhzed.solr.join;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;

/**
 * For running solr query inside any solr plugin:  must clear the threadLocal.
 * Class is lifted from solr's internal implementation named SolrRequestInfoSuspender.
 * 
 * @author mhzed
 *
 */
public class SolrSuspendedQuery extends SolrRequestInfo {
	@FunctionalInterface
	public interface QueryFunction<T> {
		T apply() throws Exception;
	}
	
  private SolrSuspendedQuery(SolrQueryRequest req, SolrQueryResponse rsp) {
    super(req, rsp);
  }
  public static <T> T run(QueryFunction<T> action) throws Exception {
   
    final SolrRequestInfo info = threadLocal.get();
    try {
      threadLocal.remove();
      return action.apply();
    } finally {
      setRequestInfo(info); 
    }
  }

  public static <T> T runUnchecked(QueryFunction<T> action) {
    try {
    	return run(action); 
    } catch (Exception e) {
    	throw new RuntimeException(e);
    }
  }
  
}
