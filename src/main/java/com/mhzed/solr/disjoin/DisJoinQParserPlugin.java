package com.mhzed.solr.disjoin;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.JoinQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;

public final class DisJoinQParserPlugin extends QParserPlugin {
	
	@Override
	public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {

		return new QParser(qstr, localParams, params, req) {
			@Override
			public Query parse() throws SyntaxError {
        List<JoinQstr> joinQueries = new ArrayList<JoinQstr>();
        for (int i = 0; true; i++) {
          String query;
          if (i == 0) {
            query = qstr; // v
          } else { // v1, v2, v3, ...
            query = localParams.get("v" + i);
          }
          if (query == null)
            break;
          joinQueries.add(JoinQstr.parse(query));
        }
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        for (int i = 0; i < joinQueries.size(); i++) {
          JoinQstr jq = joinQueries.get(i);
          for (int j = 0; j < jq.toFields.size(); j++) {
            ModifiableSolrParams lp = new ModifiableSolrParams();
            lp.set("from", jq.fromField);
            lp.set("fromIndex", jq.fromIndex);
            lp.set("to", jq.toFields.get(j));
            lp.set("v", jq.query);
            b.add(new JoinQParserPlugin().createParser(
              jq.query, lp, params, req
            ).parse(), Occur.SHOULD);
          }
        }
        return b.build();
			}
		};				

	}	
}

