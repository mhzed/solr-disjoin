package com.mhzed.solr.disjoin;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.search.SyntaxError;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerPerformanceTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(DockerPerformanceTest.class);
  static TestDockerServer dockerServer;
  public static final int BatchSize = 100000;
  public static int FolderWidth = 4;
  public static int FolderDepth = 11;   // about 1.4 million in total 
	@BeforeClass
	public static void setup() throws Exception {
    dockerServer = new TestDockerServer();
    dockerServer.launch();
    dockerServer.ensureCoresCreated();
    ingest();
    optimize();
  }
  @AfterClass
  public static void teardown() throws Exception {
    dockerServer.shutdown();
  }
  @Test
  public void basicTest() throws SyntaxError, SolrServerException, IOException {
    QueryResponse r;
    r = query(join("*:*", pathJoinQuery("/root/0/0/0/0/0/0/0/0/0/0")));
    assertEquals(5, r.getResults().getNumFound());
    r = query(join("*:*", graphJoinQuery("/root/0/0/0/0/0/0/0/0/0/0")));
    assertEquals(5, r.getResults().getNumFound());
    r = query(disJoin("*:*", new String[]{
      pathDisJoinQuery("/root/0/0/0/0/0/0/0/0/0/0")
    }, false));
    assertEquals(5, r.getResults().getNumFound());
    r = query(disJoin("*:*", new String[]{
      graphDisJoinQuery("/root/0/0/0/0/0/0/0/0/0/0")
    }, false));
    assertEquals(5, r.getResults().getNumFound());
  }
  @Test
  public void testPerformance() throws SyntaxError, SolrServerException, IOException {
    final String TestPath = "/root";
    QueryResponse r;
    LOGGER.info("start");
    r = query(join("*:*", pathJoinQuery(TestPath)));
    LOGGER.info("{}",r.getResults().getNumFound());
    r = query(join("type_s:doc", pathJoinQuery(TestPath)));
    LOGGER.info("{}",r.getResults().getNumFound());
    r = query(disJoin("*:*", new String[]{
      pathDisJoinQuery(TestPath)
    }, true));
    LOGGER.info("{}",r.getResults().getNumFound());
    r = query(disJoin("type_s:doc", new String[]{
      pathDisJoinQuery(TestPath)
    }, true));
    LOGGER.info("{}",r.getResults().getNumFound());
    // path query, solr join, disjoin, disjoin post filter
    // graph query, solr join, disjoin, disjoin post filter
  }
  QueryResponse query(SolrQuery q) throws SolrServerException, IOException {
    return dockerServer.getClient().query(TestDockerServer.FileCore, q);
  }
	SolrQuery disJoin(String mainQuery, String[] disJoinQueries, boolean postFilter) {
    String qs = IntStream.range(0, disJoinQueries.length).mapToObj(i->
      "v" + (i==0?"":i) + "=" + ClientUtils.encodeLocalParamVal(disJoinQueries[i]))
      .collect(Collectors.joining(" "));
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
            "{!disjoin %s pfsz=%d}", qs, postFilter ? -1 : (1<<30)            
            )).setRows(0).setShowDebugInfo(true);
  }
  SolrQuery join(String mainQuery, String joinQuery) {
		return new SolrQuery(mainQuery).addFilterQuery(joinQuery).setRows(0).setShowDebugInfo(true);
  }

  String pathJoinQuery(String path) {
		return String.format("{!join fromIndex=%s from=%s to=%s}%s:%s",
      TestDockerServer.FolderCore, "id", "folder_id_s",
      TestData.PathField, ClientUtils.escapeQueryChars(path)
    );
  }
  String pathDisJoinQuery(String path) {
		return String.format("%s.%s|%s|%s:%s",
      TestDockerServer.FolderCore, "id", "folder_id_s",
      TestData.PathField, ClientUtils.escapeQueryChars(path)
    );
  }
  String graphJoinQuery(String path) {
		return String.format("{!join fromIndex=%s from=%s to=%s}{!graph from=%s to=%s}%s:\"%s\"",
      TestDockerServer.FolderCore, "id", "folder_id_s",
      TestData.ParentField, TestData.IdField,
      TestData.PathField, ClientUtils.escapeQueryChars(path)
    );
  }
	String graphDisJoinQuery(String path) {
		return String.format(
      "%s.%s|%s|{!graph from=%s to=%s}%s:\"%s\"", 
      TestDockerServer.FolderCore, "id", "folder_id_s",
      TestData.ParentField, TestData.IdField, TestData.PathField, 
      ClientUtils.escapeQueryChars(path));
	}
  

  private static int nextFolderId() throws SolrServerException, IOException {
    QueryResponse r = dockerServer.getClient().query(
      TestDockerServer.FolderCore, new SolrQuery("*:*").setSort(TestData.IntField, ORDER.desc).setRows(1));
    return r.getResults().size() > 0 ? 
      (Integer)r.getResults().get(0).getFieldValue(TestData.IntField)+1: 0;
  }
  private static int nextFileFolderId() throws SolrServerException, IOException {
    String intField = "folder_" + TestData.IntField;
    QueryResponse r = dockerServer.getClient().query(
      TestDockerServer.FileCore, new SolrQuery("*:*").setSort(intField, ORDER.desc).setRows(1));
    return r.getResults().size() > 0 ? 
      (Integer)r.getResults().get(0).getFieldValue(intField)+1: 0;
  }
  private static void commit(UpdateRequest uprfolder, UpdateRequest uprdoc, SolrClient c)
      throws SolrServerException, IOException {
    uprfolder.process(c, TestDockerServer.FolderCore);
    uprdoc.process(c, TestDockerServer.FileCore);
    c.commit(TestDockerServer.FolderCore);
    c.commit(TestDockerServer.FileCore);
    uprfolder.clear();
    uprdoc.clear();
  }
  public static void ingest() throws Exception {
    SolrClient c = dockerServer.getClient();
    UpdateRequest uprfolder = new UpdateRequest();
    UpdateRequest uprdoc = new UpdateRequest();
    // allow resumable ingestion
    int cur = Math.min(nextFolderId(), nextFileFolderId());
    int totalSize = TestData.branchSize(FolderWidth, FolderDepth);
    if (cur < totalSize) {
      LOGGER.info("Starting ingestion at {}", (cur));
      int size = TestData.branch("/root", -1, 0, FolderWidth, FolderDepth, (folder, id)->{
        if (id >= cur) {
          uprfolder.add(folder);    
          uprdoc.add(TestData.docInFolder(id));  
          if (id >0 && id % BatchSize == 0) {
            commit(uprfolder, uprdoc, c);
            LOGGER.info("{}({}%) files ingested", id, 
              (int)((id)*100/totalSize) );
          }
        }
      });
      commit(uprfolder, uprdoc, c);
      LOGGER.info("Ingestion done: {} files ingested", size);
    } else {
      LOGGER.info("Ingestion was completed");
    }
  }
  public static void optimize() throws Exception {
    SolrClient c = dockerServer.getClient();
    c.optimize(TestDockerServer.FolderCore);
    c.optimize(TestDockerServer.FileCore);
    LOGGER.info("Optimized");
  }
}