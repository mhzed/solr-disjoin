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
import org.apache.solr.search.SyntaxError;
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
  public void testQstrParser() throws SyntaxError {
    JoinQstr p1 = JoinQstr.parse("id|toid|path:abc");
    assertEquals(p1.fromIndex, null);
    assertEquals(p1.fromField, "id");
    assertEquals(p1.toFields.get(0), "toid");
    assertEquals(p1.query, "path:abc");
    
    JoinQstr p2 = JoinQstr.parse("folders.id|toid|path:abc");
    assertEquals(p2.fromIndex, "folders");
    assertEquals(p2.fromField, "id");
    assertEquals(p2.toFields.get(0), "toid");
    assertEquals(p2.query, "path:abc");

    JoinQstr p3 = JoinQstr.parse("fol.ders.id|toid,id2|path:abc");
    assertEquals(p3.fromIndex, "fol.ders");
    assertEquals(p3.fromField, "id");
    assertEquals(p3.toFields.get(0), "toid");
    assertEquals(p3.toFields.get(1), "id2");
    assertEquals(p3.query, "path:abc"); 
  }
  @Test(expected = SyntaxError.class)
  public void testQstrParserError() throws SyntaxError {
    JoinQstr.parse("fol.ders.id|toid");
  }

	@Test
	public void test() throws Exception {
		List<SolrInputDocument> folders = branch("", null, 0, 3, 3);	// size: 3^1 + 3^2 + 3^3 = 39
    new UpdateRequest().add(folders).process(client, FolderCollection);
    client.commit(FolderCollection);
		new UpdateRequest().add(docs(folders)).process(client, DocCollection);
    client.commit(DocCollection);
    new UpdateRequest().add(folders).process(client, SingleDocFolderCollection);
    new UpdateRequest().add(docs(folders)).process(client, SingleDocFolderCollection);
    client.commit(SingleDocFolderCollection);
  
    testWithCacheInspection();
    testJoinWithNone(false);
    testJoinWithNone(true);
    testGraph(false);
    testGraph(true);
    testDisjoin(false);
    testDisjoin(true);

    testSameCollectionJoin();    
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
    r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s"),
      pathQuery("/2/0", "id_i", "folder_id_i"),
      pathQuery("/0/0", "id_l", "folder_id_l")
    }, postFilter));
    assertEquals(12, r.getResults().size());
    r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s"),
      pathQuery("/2/0", "id", "folder_id_s")
    }, postFilter));
    assertEquals(8, r.getResults().size());
    r = client.query(DocCollection, disJoin("*:*", new String[]{
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
  public void testToTypeError() throws SolrServerException, IOException {
    client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id_l", "folder_id_l,link_folder_id_s")
    }, false));
  }

	SolrQuery disJoin(String mainQuery, String[] joinQueries, boolean postFilter) {
    String qs = IntStream.range(0, joinQueries.length).mapToObj(i->
      "v" + (i==0?"":i) + "=" + ClientUtils.encodeLocalParamVal(joinQueries[i])).collect(Collectors.joining(" "));
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
            "{!djoin %s pfsz=%d}", qs, postFilter ? 0 : (1<<30)            
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
	List<SolrInputDocument> branch(String path, Integer parentId, int idoffset, int width, int depth) {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		if (depth <= 0) return docs;
		int id = idoffset;
		
		List<Integer> childrenIds = new ArrayList<Integer>();
		for (int w=0; w<width; w++) {
			docs.add(docOf(IdField, String.valueOf(id), IntField, id, LongField, id, 
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
	List<SolrInputDocument> docs(List<SolrInputDocument> folders) {
    
		return folders.stream().map(folder-> {
      Integer id = (Integer)folder.getFieldValue(IntField);
			return docOf("folder_" + IdField + "_s", String.valueOf(id), 
							"folder_" + IntField, id,
              "folder_" + LongField, Long.valueOf(id),
              "link_folder_" + IdField + "_s", String.valueOf(id+1), 
							"link_folder_" + IntField, id+1,
              "link_folder_" + LongField, Long.valueOf(id+1),
              "type_s", "doc" );
    }).collect(Collectors.toList());
		
	}
	
	public static SolrInputDocument docOf(Object... args) {
		SolrInputDocument doc = new SolrInputDocument();
		for (int i = 0; i < args.length; i += 2) {
			doc.addField(args[i].toString(), args[i + 1]);
		}
		return doc;
	}
		
}
