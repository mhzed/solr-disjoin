package com.mhzed.solr.join;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.RefCounted;

public final class FilterJoinQParserPlugin extends QParserPlugin {

	//{!join q=path:abc col=col1 on=field:filter_field pfsz=1000}
	public static final String ParamQuery = "q";
	public static final String ParamCollection = "fromCollection";
	public static final String ParamFrom = "from";
	public static final String ParamTo = "to";
	public static final String ParamPostFilterSize = "pfsz";
	
	private static final String DefaultPostFilterSize = "1000";
	
	@Override
	public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {

		String thisCollection = req.getCore().getCoreDescriptor().getCollectionName();
		String query = qstr;
		String fromField = localParams.get(ParamFrom);
		String toField = localParams.get(ParamTo);
		// optional params
		String fromCollection = localParams.get(ParamCollection, thisCollection);
		int pfsz = Integer.parseInt(localParams.get(ParamPostFilterSize, DefaultPostFilterSize));
				
		SolrCore fromCore = req.getCore().getCoreContainer().getCore(fromCollection);
		RefCounted<SolrIndexSearcher> fromSearcher = fromCore.getSearcher(false, true, null);
		SolrRequestInfo.getRequestInfo().addCloseHook(new Closeable() {
      @Override
      public void close() {
      	fromSearcher.decref();
      	fromCore.close();
      }
    });
		
		// fromSearcher.get().
		// TODO
		CloudSolrClient client = new CloudSolrClient.Builder(Arrays.asList(
				req.getCore().getCoreContainer().getZkController().getZkClient().getZkServerAddress()
			), Optional.empty()
		).build();
		
		Set<Object> joinVals = SolrSuspendedQuery.runUnchecked(()->{
			SolrQuery q = new SolrQuery(query).addField(fromField).setRows(Integer.MAX_VALUE);
			return client.query(fromCollection, q).getResults().stream()
				.map((d)->d.getFieldValue(fromField)).collect(Collectors.toSet());
		});
		FieldType fieldType = req.getSchema().getField(toField).getType();
		return new QParser(qstr, localParams, params, req) {
			@Override
			public Query parse() throws SyntaxError {
				return FilterQueryFactory.createQuery(toField, fieldType, joinVals, pfsz);
			}
		};				
	}	
}

