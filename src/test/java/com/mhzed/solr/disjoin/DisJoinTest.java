package com.mhzed.solr.disjoin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
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
public class DisJoinTest extends SolrCloudTestCase {
	static CloudSolrClient client;
	static final int NodeCount = 5;
	
	static final String DocCollection = "docs";
  static final String FolderCollection = "folders";
  static final String SingleDocFolderCollection = "docfolders";
	
	static final String PathField = "path_descendent_path";
	static final String IdField = "id";	// for test join by string
	static final String IntField = "id_i";	// for test join by integer
  static final String LongField = "id_l";	// for test join by long
  static final String DoubleField = "id_d";	// for test join by double
  static final String TextField = "id_t"; // for unsuported type test 
	static final String ParentField = "parent_id_s";
	static {
		System.setProperty("java.security.egd", "file:/dev/./urandom");		
    System.setProperty("solr.log.dir", "./logs");
	}
	
	@BeforeClass
	public static void setup() throws Exception {
		Builder builder = configureCluster(NodeCount);

		Path p = new File(DisJoinTest.class.getResource("../../../../test_core/conf").toURI()).toPath();
		builder.addConfig("_default", p);
    builder.configure();
		cluster.waitForAllNodes(60000);
		client = cluster.getSolrClient();
		
		CollectionAdminRequest.createCollection(
			DocCollection, "_default", NodeCount - 2 , 1).process(client);
		CollectionAdminRequest.createCollection(
      FolderCollection, "_default", 1, NodeCount).process(client);      
    CollectionAdminRequest.createCollection(
      SingleDocFolderCollection, "_default", 1, NodeCount).process(client);
  
    List<SolrInputDocument> folders = branch("", null, 0, 3, 3);	// size: 3^1 + 3^2 + 3^3 = 39
    new UpdateRequest().add(folders).process(client, FolderCollection);
    client.commit(FolderCollection);

    List<SolrInputDocument> folderdocs = docs(folders);
    ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    docs.addAll(randDocs(11));
    docs.addAll(folderdocs.subList(0, 10));
    new UpdateRequest().add(docs).process(client, DocCollection);
    docs.clear();
    docs.addAll(randDocs(300));
    docs.addAll(folderdocs.subList(10, folderdocs.size()));
    docs.addAll(randDocs(7));
    new UpdateRequest().add(docs).process(client, DocCollection);
    client.commit(DocCollection);

    new UpdateRequest().add(folders).process(client, SingleDocFolderCollection);
    new UpdateRequest().add(docs(folders)).process(client, SingleDocFolderCollection);
    client.commit(SingleDocFolderCollection);      
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
    testWithCacheInspection();
    
    testGraph(false);
    testGraph(true);
    testDisjoin(false);
    testDisjoin(true);
    testMvJoin();
    testSameCollectionJoin();    

    testJoinWithNone(true);
    testJoinWithNone(false);

  }
  private void testWithCacheInspection() throws SolrServerException, IOException {
		QueryResponse r;
		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0", "id", "folder_id_s")}, false));
		assertEquals(13, r.getResults().size());
		assertTrue(isFilter(r.getDebugMap()));
		
		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0", "id", "folder_id_s")}, true));
		assertEquals(13, r.getResults().size());
		assertTrue(isCached(r.getDebugMap()));	// even if post-filter is specified, query cache catches it
		
		r = client.query(DocCollection, disJoin("id:*", new String[]{
      pathQuery("/0", "id", "folder_id_s")}, false));
		assertEquals(13, r.getResults().size());
		assertTrue(isCached(r.getDebugMap()));	// main query changed, but filter query should still be cached

		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s")}, true));
		assertEquals(4, r.getResults().size());
		assertTrue(isPostFilter(r.getDebugMap()));
		
		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s")}, false));
		assertEquals(4, r.getResults().size());
		assertTrue(isCached(r.getDebugMap()));	// query cache catches it
		
		r = client.query(DocCollection, disJoin("id:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s")}, true));
		assertEquals(4, r.getResults().size());
		assertTrue(isPostFilter(r.getDebugMap()));	// main query changed, post filter invoked again 

  }
  private void testJoinWithNone(boolean postFilter) throws SolrServerException, IOException {
    QueryResponse r;
		r = client.query(DocCollection, disJoin("id:*", new String[]{
      pathQuery("/100", "id", "folder_id_s")}, postFilter));
		assertEquals(0, r.getResults().size());
  }
  private void testSameCollectionJoin() throws SolrServerException, IOException {
    QueryResponse r;
		r = client.query(SingleDocFolderCollection, disJoin("*:*", new String[]{
      pathQuery(SingleDocFolderCollection, "/0", "id", "folder_id_s")}, false));
		assertEquals(13, r.getResults().size());
  }
  private void testGraph(boolean postFilter) throws SolrServerException, IOException {
    QueryResponse r;
		r = client.query(DocCollection, disJoin("*:*", new String[]{graphQuery("0")}, postFilter));
		assertEquals(13, r.getResults().size());

    r = client.query(DocCollection, disJoin("*:*", new String[]{graphQuery("3")}, postFilter));
    assertEquals(4, r.getResults().size());
  }
  private void testDisjoin(boolean postFilter) throws SolrServerException, IOException {
    QueryResponse r;
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1/0", "id", "folder_id_s"),
      pathQuery("/2/0", "id_i", "folder_id_i"),
      pathQuery("/0/0", "id_d", "folder_id_d")
    }, postFilter));
    assertEquals(12, r.getResults().size());
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1/0", "id", "folder_id_s"),
      pathQuery("/2/0", "id", "folder_id_s")
    }, postFilter));
    assertEquals(8, r.getResults().size());
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1", "id_l", "folder_id_l"),
      graphQuery("0")
    }, postFilter));
    assertEquals(26, r.getResults().size());
    r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id_l", "folder_id_l"),
      graphQuery("0")
    }, postFilter));
    assertEquals(13, r.getResults().size());

    r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id_l", "folder_id_l,link_folder_id_l")
    }, postFilter));
    assertEquals(4+1*2, r.getResults().size());    // 1 is link_*_id offset from id

  }
  @Test(expected = SolrServerException.class)
  public void testToTypesNotSameError() throws SolrServerException, IOException {
    client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id_l", "folder_id_l,link_folder_id_s")
    }, false));
  }
  @Test(expected = SolrServerException.class)
  public void testJoinTypeNoMatchError() throws SolrServerException, IOException {
    client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id", "folder_id_i")
    }, false));
  }
  @Test(expected = SolrServerException.class)
  public void testJoinTypeUnsupportedError() throws SolrServerException, IOException {
    client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id_t", "folder_id_t")
   
    }, false));
  }
  // void testDelByQuery() throws SolrServerException, IOException {
  //   String q = new SolrQuery("*:*").addFilterQuery(String.format("{!djoin}%s",
  //     pathQuery("/2/2/0", "id_ss", "folder_id_ss"))).toQueryString();
  //   client.deleteByQuery(DocCollection, q);
  //   client.commit(DocCollection);
	// 	QueryResponse r = client.query(DocCollection, disJoin("*:*", new String[]{
  //     pathQuery("/2/2", "id", "folder_id_s")}, false));
	// 	assertEquals(3, r.getResults().size());

  // }
  void testMvJoin() throws SolrServerException, IOException {
    QueryResponse r;
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1/0", "id_ss", "folder_id_ss"),
      pathQuery("/2/0", "id_is", "folder_id_is"),
      pathQuery("/0/0", "id_ls", "folder_id_ls"),
      pathQuery("/0/2", "id_ds", "folder_id_ds")
    }, false));
    assertEquals(16, r.getResults().size());
  }

	SolrQuery disJoin(String mainQuery, String[] joinQueries, boolean postFilter) {
    String qs = IntStream.range(0, joinQueries.length).mapToObj(i->
      "v" + (i==0?"":i) + "=" + ClientUtils.encodeLocalParamVal(joinQueries[i])).collect(Collectors.joining(" "));
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
            "{!djoin %s pfsz=%d}", qs, postFilter ? -1 : (1<<30)            
						)).setRows(10000).setShowDebugInfo(true);

  }
	String graphQuery(String id) {
		return String.format(
						"%s.%s|%s|{!graph from=%s to=%s}%s:%s", 
						FolderCollection, "id", "folder_id_s",
						ParentField, IdField, IdField, ClientUtils.escapeQueryChars(id));
	}

  String pathQuery(String fromIndex, String path, String from, String to) {
		return String.format("%s.%s|%s|%s",
      fromIndex,
      from,
      to,
      PathField + ":" + ClientUtils.escapeQueryChars(path)
    );
	}

	String pathQuery(String path, String from, String to) {
		return pathQuery(FolderCollection, path, from, to);
	}
		
	
	// generate test folder docs
	static List<SolrInputDocument> branch(String path, Integer parentId, int idoffset, int width, int depth) {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		if (depth <= 0) return docs;
		int id = idoffset;
		
		List<Integer> childrenIds = new ArrayList<Integer>();
		for (int w=0; w<width; w++) {
      docs.add(docOf(IdField, String.valueOf(id), IntField, id, LongField, id, DoubleField, id, TextField, id,
              "id_ss", String.valueOf(id), IntField+'s', id, LongField+'s', id, DoubleField+'s', id,
							PathField, path + "/" + w, ParentField, parentId, "type_s", "folder"));
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
	static List<SolrInputDocument> docs(List<SolrInputDocument> folders) {
    
		return folders.stream().map(folder-> {
      Integer id = (Integer)folder.getFieldValue(IntField);
			return docOf(
        "folder_" + IdField + "_s", String.valueOf(id), 
        "folder_" + IntField, id,
        "folder_" + LongField, Long.valueOf(id),
        "folder_" + DoubleField, Double.valueOf(id),
        "folder_" + TextField, id,
        "folder_" + IdField + "_ss", String.valueOf(id), 
        "folder_" + IntField + "s", id,
        "folder_" + LongField + "s", Long.valueOf(id),
        "folder_" + DoubleField + "s", Double.valueOf(id),
        "link_folder_" + IdField + "_s", String.valueOf(id+1), 
        "link_folder_" + IntField, id+1,
        "link_folder_" + LongField, Long.valueOf(id+1),
        "type_s", "doc" );
    }).collect(Collectors.toList());
		
  }
  static List<SolrInputDocument> randDocs(int n) {
    return IntStream.range(0, n).mapToObj(i->
      docOf("random_d", Math.random(), "random_t", Math.random())
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
