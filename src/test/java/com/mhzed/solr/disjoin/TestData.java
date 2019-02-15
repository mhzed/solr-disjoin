package com.mhzed.solr.disjoin;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;

class TestData {
  static final String PathField = "path_descendent_path";
  static final String PathStringField = "path_s";
	static final String IdField = "id";	// for test join by string
	static final String IntField = "id_i";	// for test join by integer
  static final String LongField = "id_l";	// for test join by long
  static final String DoubleField = "id_d";	// for test join by double
  static final String TextField = "id_t"; // for unsuported type test 
  static final String ParentField = "parent_id_s";
  static final String ParentLongField = "parent_id_l";

  @FunctionalInterface
  interface DocSink {
    void apply(SolrInputDocument doc, int id) throws Exception;
  }
  static int branchSize( int width, int depth) {
    int sum = 0;
    for (int i=1; i<=depth; i++) {
      sum += Math.pow(width, i);
    }
    return sum;
  }
  /**
   * generate a uniform tree of test folders breadth first, i.e.
   *    0      1 
   *  2   3  4   5
   * 6 7 8 9 ....
   * return number of folders generated, = sum(width^1, width^2, ... width^(depth))
   */
  static int branch(String path, Integer parentId, int idoffset, 
    int width, int depth, DocSink sink) throws Exception {

		if (depth <= 0) return 0;
		int id = idoffset;
		
		List<Integer> childrenIds = new ArrayList<Integer>();
		for (int w=0; w<width; w++) {
      sink.apply(folder(id, path + "/" + w, parentId), id);
			childrenIds.add(id);
			id++;
		}
		for (int w=0; w<width; w++) {
			id += branch(path + "/" + w, childrenIds.get(w), id, width, depth-1, sink);
		}
		return id-idoffset;
  }
  static SolrInputDocument folder(Integer id, String path, Integer parentId) {
    return docOf(IdField, String.valueOf(id), IntField, id, LongField, id, DoubleField, id, TextField, id,
    "id_ss", String.valueOf(id), IntField+'s', id, LongField+'s', id, DoubleField+'s', id,
    PathField, path, PathStringField, path, ParentField, parentId, 
    ParentLongField, parentId, "type_s", "folder");    
  }
  static SolrInputDocument docInFolder(Integer folderId) {
    return docOf(
      "folder_" + IdField + "_s", String.valueOf(folderId), 
      "folder_" + IntField, folderId,
      "folder_" + LongField, Long.valueOf(folderId),
      "folder_" + DoubleField, Double.valueOf(folderId),
      "folder_" + TextField, folderId,
      "folder_" + IdField + "_ss", String.valueOf(folderId), 
      "folder_" + IntField + "s", folderId,
      "folder_" + LongField + "s", Long.valueOf(folderId),
      "folder_" + DoubleField + "s", Double.valueOf(folderId),
      "link_folder_" + IdField + "_s", String.valueOf(folderId+1), 
      "link_folder_" + IntField, folderId+1,
      "link_folder_" + LongField, Long.valueOf(folderId+1),
      "type_s", "doc" );
  }

	public static SolrInputDocument docOf(Object... args) {
		SolrInputDocument doc = new SolrInputDocument();
		for (int i = 0; i < args.length; i += 2) {
			doc.addField(args[i].toString(), args[i + 1]);
		}
		return doc;
	}

	public static SolrQuery disJoin(String mainQuery, String[] joinQueries) {
    String qs = IntStream.range(0, joinQueries.length).mapToObj(i->
      "should=" + ClientUtils.encodeLocalParamVal(joinQueries[i])).collect(Collectors.joining(" "));
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
            "{!bool %s}", qs
						)).setRows(0).setShowDebugInfo(true);
  }
	
	public static String graphJoinQuery(
			String fromCollection, String fromId, String toId,
			String fromGraphField, String toGraphField, String qField, String id) {
		return String.format(
						"{!join fromIndex=%s from=%s to=%s}{!graph from=%s to=%s}%s:\"%s\"",
						fromCollection, fromId, toId,
						fromGraphField, toGraphField, qField, 
						ClientUtils.escapeQueryChars(id));
	}

  
	public static String pathJoinQuery(String fromCollection, String from, 
  				String to, String qField, String path) {
		return String.format("{!join fromIndex=%s from=%s to=%s}%s:\"%s\"",
						fromCollection,
			      from,
			      to,
			      qField, ClientUtils.escapeQueryChars(path)
			    );  	
  }

}