package com.mhzed.solr.disjoin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
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
      FolderCollection, "_default", 1, NodeCount-2).process(client);
    CollectionAdminRequest.Create createDocs = CollectionAdminRequest.createCollection(
      DocCollection, "_default", NodeCount - 2 , 1);
    createDocs.setWithCollection(FolderCollection);
    createDocs.process(client);      
    CollectionAdminRequest.createCollection(
      SingleDocFolderCollection, "_default", 1, NodeCount).process(client);
  
    // size: 3^1 + 3^2 + 3^3 = 39
    
    UpdateRequest uprfolder = new UpdateRequest();
    uprfolder.add(TestData.folder(-1, "", null));  // inject a root folder, no doc under it
    UpdateRequest uprdoc = new UpdateRequest();
    TestData.branch("", -1, 0, 3, 3, (doc, id)->{
      uprfolder.add(doc);    
      uprdoc.add(TestData.docInFolder(id));  
    });	
    uprfolder.process(client, FolderCollection);
    client.commit(FolderCollection);    
    uprdoc.process(client, DocCollection);
    client.commit(DocCollection);

    uprfolder.process(client, SingleDocFolderCollection);
    uprdoc.process(client, SingleDocFolderCollection);
    client.commit(SingleDocFolderCollection);      
	}
  @AfterClass
  public static void teardown() throws Exception {
  }	
  
	@Test
	public void test() throws Exception {
    testSingle();
    testGraphJoin();
    testPathJoin();
    testMvJoin();
    testSameCollectionJoin();    
    testJoinWithNone();
    testDelByQuery();
  }
  private void testSingle() throws SolrServerException, IOException {
		QueryResponse r;
		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0", "id", "folder_id_s")}));
		assertEquals(13, r.getResults().getNumFound());
		
		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0", "id", "folder_id_s")}));
		assertEquals(13, r.getResults().getNumFound());
		
		r = client.query(DocCollection, disJoin("id:*", new String[]{
      pathQuery("/0", "id", "folder_id_s")}));
		assertEquals(13, r.getResults().getNumFound());

		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s")}));
		assertEquals(4, r.getResults().getNumFound());
		
		r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s")}));
		assertEquals(4, r.getResults().getNumFound());
		
		r = client.query(DocCollection, disJoin("id:*", new String[]{
      pathQuery("/1/0", "id", "folder_id_s")}));
		assertEquals(4, r.getResults().getNumFound());

  }
  private void testJoinWithNone() throws SolrServerException, IOException {
    QueryResponse r;
		r = client.query(DocCollection, disJoin("id:*", new String[]{
      pathQuery("/100", "id", "folder_id_s")}));
		assertEquals(0, r.getResults().getNumFound());
  }
  private void testSameCollectionJoin() throws SolrServerException, IOException {
    QueryResponse r;
		r = client.query(SingleDocFolderCollection, disJoin("*:*", new String[]{
      pathQuery(SingleDocFolderCollection, "/0", "id", "folder_id_s")}));
		assertEquals(13, r.getResults().getNumFound());
  }
  private void testGraphJoin() throws SolrServerException, IOException {
    QueryResponse r;
		r = client.query(DocCollection, disJoin("*:*", new String[]{graphQuery("0")}));
		assertEquals(13, r.getResults().getNumFound());

    r = client.query(DocCollection, disJoin("*:*", new String[]{graphQuery("3")}));
    assertEquals(4, r.getResults().getNumFound());

    r = client.query(DocCollection, disJoin("*:*", new String[]{graphQuery("-1")}));
    assertEquals(39, r.getResults().getNumFound());

  }
  private void testPathJoin() throws SolrServerException, IOException {
    QueryResponse r;
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1/0", "id", "folder_id_s"),
      pathQuery("/2/0", "id_i", "folder_id_i"),
      pathQuery("/0/0", "id_d", "folder_id_d")
    }));
    assertEquals(12, r.getResults().getNumFound());
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1/0", "id", "folder_id_s"),
      pathQuery("/2/0", "id", "folder_id_s")
    }));
    assertEquals(8, r.getResults().getNumFound());
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1", "id_l", "folder_id_l"),
      graphQuery("0")
    }));
    assertEquals(26, r.getResults().getNumFound());
    r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id_l", "folder_id_l"),
      graphQuery("0")
    }));
    assertEquals(13, r.getResults().getNumFound());

    r = client.query(DocCollection, disJoin("*:*", new String[]{
      pathQuery("/0/1", "id_l", "folder_id_l,link_folder_id_l")
    }));
    assertEquals(4+1*2, r.getResults().getNumFound());    // 1 is link_*_id offset from id

  }

  void testDelByQuery() throws SolrServerException, IOException {
    // String q = new SolrQuery("*:*").addFilterQuery(String.format("{!disjoin}%s",
    //   pathQuery("/2/2/0", "id_ss", "folder_id_ss"))).toQueryString();
    // client.deleteByQuery(DocCollection, q);
    // client.commit(DocCollection);
		// QueryResponse r = client.query(DocCollection, disJoin("*:*", new String[]{
    //   pathQuery("/2/2", "id", "folder_id_s")}, false));
		// assertEquals(3, r.getResults().getNumFound());

  }
  void testMvJoin() throws SolrServerException, IOException {
    QueryResponse r;
    r = client.query(DocCollection, disJoin("type_s:doc", new String[]{
      pathQuery("/1/0", "id_ss", "folder_id_ss"),
      pathQuery("/2/0", "id_is", "folder_id_is"),
      pathQuery("/0/0", "id_ls", "folder_id_ls"),
      pathQuery("/0/2", "id_ds", "folder_id_ds")
    }));
    assertEquals(16, r.getResults().getNumFound());
  }

	SolrQuery disJoin(String mainQuery, String[] joinQueries) {
    String qs = IntStream.range(0, joinQueries.length).mapToObj(i->
      "v" + (i==0?"":i) + "=" + ClientUtils.encodeLocalParamVal(joinQueries[i])).collect(Collectors.joining(" "));
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
            "{!disjoin %s}", qs
						)).setRows(0).setShowDebugInfo(true);

  }
	String graphQuery(String id) {
		return String.format(
						"%s.%s|%s|{!graph from=%s to=%s}%s:\"%s\"", 
						FolderCollection, "id", "folder_id_s",
						TestData.ParentField, TestData.IdField, TestData.IdField, ClientUtils.escapeQueryChars(id));
	}

  String pathQuery(String fromIndex, String path, String from, String to) {
		return String.format("%s.%s|%s|%s",
      fromIndex,
      from,
      to,
      TestData.PathField + ":" + ClientUtils.escapeQueryChars(path)
    );
	}

	String pathQuery(String path, String from, String to) {
		return pathQuery(FolderCollection, path, from, to);
	}
		
  static List<SolrInputDocument> randDocs(int n) {
    return IntStream.range(0, n).mapToObj(i->
      TestData.docOf("random_d", Math.random(), "random_t", Math.random())
    ).collect(Collectors.toList());
  }
			
}
