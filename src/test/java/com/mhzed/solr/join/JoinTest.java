package com.mhzed.solr.join;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 */
public class JoinTest extends SolrCloudTestCase {
	static CloudSolrClient client;
	static final int NodeCount = 5;
	
	static final String DocCollection = "docs";
	static final String SingleFolderCollection = "folders";
	static final String ShardedFolderCollection = "foldershards";
	
	static final String PathField = "path_descendent_path";
	static final String IdField = "id";	// for test join by string
	static final String IntField = "id_i";	// for test join by integer
	static final String LongField = "id_l";	// for test join by long
	static final String ParentField = "parent_id_s";
	static {
		System.setProperty("java.security.egd", "file:/dev/./urandom");		
		System.setProperty("solr.log.dir", "./logs");
	}
	
	@BeforeClass
	public static void setup() throws Exception {
		Builder builder = configureCluster(NodeCount);
		
		Path p = new File(JoinTest.class.getResource("../../../../test_core/conf").toURI()).toPath();
		builder.addConfig("_default", p);
    builder.configure();
		cluster.waitForAllNodes(60000);
		client = cluster.getSolrClient();
		
		CollectionAdminRequest.createCollection(
						DocCollection, "_default", NodeCount - 2 , 1).process(client);
		CollectionAdminRequest.createCollection(
						SingleFolderCollection, "_default", 1, NodeCount).process(client);
		CollectionAdminRequest.createCollection(
						ShardedFolderCollection, "_default", NodeCount - 2 , 1).process(client);

	}
  @AfterClass
  public static void teardown() throws Exception {
  }	
  
  public boolean isFilter(Map<String, Object> debugMap) {
  	return debugMap.toString().contains("{FilterJoin={={type=filter,");
  }
  public boolean isPostFilter(Map<String, Object> debugMap) {
  	return debugMap.toString().contains("{type=post-filter,");
  }
  public boolean isCached(Map<String, Object> debugMap) {
  	return !isFilter(debugMap) && !isPostFilter(debugMap);
  }
  
	@Test
	public void test() throws Exception {
		List<SolrInputDocument> folders = branch("", null, 0, 3, 3);	// size: 3^1 + 3^2 + 3^3 = 39
		new UpdateRequest().add(folders).process(client, SingleFolderCollection);
		client.commit(SingleFolderCollection);
		new UpdateRequest().add(docs(folders)).process(client, DocCollection);
		client.commit(DocCollection);
	
		QueryResponse r;
		r = client.query(DocCollection, joinFilterQuery("/0", "id", "folder_id_s", false));
		assertEquals(13, r.getResults().size());
		assertTrue(isFilter(r.getDebugMap()));
		
		r = client.query(DocCollection, joinFilterQuery("/0", "id", "folder_id_s", true));
		assertEquals(13, r.getResults().size());
		assertTrue(isCached(r.getDebugMap()));	// even if post-filter is specified, query cache catches it
		
		r = client.query(DocCollection, joinFilterQuery("id:*", "/0", "id", "folder_id_s", false));
		assertEquals(13, r.getResults().size());
		assertTrue(isCached(r.getDebugMap()));	// main query changed, but filter query should still be cached

		r = client.query(DocCollection, joinFilterQuery("/1/0", "id", "folder_id_s", true));
		assertEquals(4, r.getResults().size());
		assertTrue(isPostFilter(r.getDebugMap()));
		
		r = client.query(DocCollection, joinFilterQuery("/1/0", "id", "folder_id_s", false));
		assertEquals(4, r.getResults().size());
		assertTrue(isCached(r.getDebugMap()));	// query cache catches it
		
		r = client.query(DocCollection, joinFilterQuery("id:*", "/1/0", "id", "folder_id_s", true));
		assertEquals(4, r.getResults().size());
		assertTrue(isPostFilter(r.getDebugMap()));	// main query changed, post filter invoked again 

		r = client.query(DocCollection, graphJoinQuery("*:*", "0"));
		assertEquals(13, r.getResults().size());
		
	}
	
	SolrQuery graphJoinQuery(String mainQuery, String id) {
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
						"{!fjoin fromIndex=%s from=%s to=%s}{!graph from=%s to=%s}%s:%s", 
						SingleFolderCollection, "id", "folder_id_s",
						ParentField, IdField, IdField, ClientUtils.escapeQueryChars(id))).setRows(10000);
	}
	
	SolrQuery joinFilterQuery(String mainQuery, String path,
					String from, String to, boolean postFilter) {
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
						"{!fjoin fromIndex=%s from=%s to=%s pfsz=%d}%s",
						SingleFolderCollection,
						ClientUtils.encodeLocalParamVal(from),
						ClientUtils.encodeLocalParamVal(to),
						postFilter ? 0 : (1<<30),
						PathField + ":" + ClientUtils.escapeQueryChars(path)
						)).setRows(10000).setShowDebugInfo(true);
	}
	SolrQuery joinFilterQuery(String path,
					String from, String to, boolean postFilter) {
		return joinFilterQuery("*:*", path, from, to, postFilter);
	}
		
	
	// generate test folder docs
	List<SolrInputDocument> branch(String path, Integer parentId, int idoffset, int width, int depth) {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		if (depth <= 0) return docs;
		int id = idoffset;
		
		List<Integer> childrenIds = new ArrayList<Integer>();
		for (int w=0; w<width; w++) {
			docs.add(docOf(IdField, String.valueOf(id), IntField, id, LongField, id, 
							PathField, path + "/" + w, ParentField, parentId));
			childrenIds.add(id);
			id++;
		}
		for (int w=0; w<width; w++) {
			List<SolrInputDocument> b = branch(path + "/" + w, childrenIds.get(w), id, width, depth-1);
			id += b.size();
			docs.addAll(b);
		}
		return docs;
	}
	// generate 1 doc for each folder 
	List<SolrInputDocument> docs(List<SolrInputDocument> folders) {
		return folders.stream().map(folder->
			docOf("folder_" + IdField + "_s", folder.getFieldValue(IdField), 
							"folder_" + IntField, folder.getFieldValue(IntField),
							"folder_" + LongField, folder.getFieldValue(LongField))
		).collect(Collectors.toList());
		
	}
	
	public static SolrInputDocument docOf(Object... args) {
		SolrInputDocument doc = new SolrInputDocument();
		for (int i = 0; i < args.length; i += 2) {
			doc.addField(args[i].toString(), args[i + 1]);
		}
		return doc;
	}
		
}
