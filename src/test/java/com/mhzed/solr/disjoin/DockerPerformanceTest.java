package com.mhzed.solr.disjoin;


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
  public static int FolderDepth = 11;   // about 5.6 million in total 
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
  public void test() throws SyntaxError, SolrServerException, IOException {

    runPathJoin("/root");
    runPathJoin("/root/0");
    runPathJoin("/root/1");
    runPathJoin("/root/2");
    runPathJoin("/root/3");
    runPathJoin("/root/0/0");
    runPathJoin("/root/0/0/0/0/0/0");
    runPathJoin("/root/0/0/0/0/0/0/0/0/0/0");

    runPathJoinInt("/root");    
    runPathJoinInt("/root/0");
    runPathJoinInt("/root/1");
    runPathJoinInt("/root/2");
    runPathJoinInt("/root/3");
    runPathJoinInt("/root/2/1");
    runPathJoinInt("/root/0/0");
    runPathJoinInt("/root/3/3");
    runPathJoinInt("/root/1/0");
    runPathJoinInt("/root/1/1/2");
    runPathJoinInt("/root/3/3/0");
    runPathJoinInt("/root/2/1/0");
    runPathJoinInt("/root/0/0/0/0/0/0");
    runPathJoinInt("/root/0/0/0/0/0/0/0/0/0/0");

    QueryResponse r;
    r = query(disJoin("type_s:doc", new String[]{
      pathDisJoinQuery("/root/0", "id", "folder_id_s"),
      pathDisJoinQuery("/root/1", "id", "folder_id_s"),
      pathDisJoinQuery("/root/2", "id", "folder_id_s"),
      pathDisJoinQuery("/root/3", "id", "folder_id_s")
    }));
    report("PathToken: dis-join of 4 queries", r);

    runGraphQueryCompare("/root/0");
    runGraphQueryCompare("/root/0/0");
  }
  private void report(String line, QueryResponse r) {
    System.out.println(String.format("%s. Size %d took %dms", line, r.getResults().getNumFound(), r.getQTime()));
  }
  public void runPathJoin(String path) throws SyntaxError, SolrServerException, IOException {
    QueryResponse r;
    r = query(disJoin("type_s:doc", new String[]{
      pathDisJoinQuery(path, "id", "folder_id_s")
    }));
    report("PathToken(str)", r);
  }
  public void runPathJoinInt(String path) throws SyntaxError, SolrServerException, IOException {
    QueryResponse r;
    r = query(disJoin("type_s:doc", new String[]{
      pathDisJoinQuery(path, "id_l", "folder_id_l")
    }));
    report("PathToken(int)", r);
  }
  public void runGraphQueryCompare(String path) throws SyntaxError, SolrServerException, IOException {    
    QueryResponse r;
    r = dockerServer.getClient().query(TestDockerServer.FolderCore, new SolrQuery(
      String.format("{!graph from=%s to=%s}%s:\"%s\"", 
      TestData.ParentField, TestData.IdField, TestData.PathStringField, 
      ClientUtils.escapeQueryChars(path))));
    report("Graph", r);
    r = dockerServer.getClient().query(TestDockerServer.FolderCore, new SolrQuery(
      String.format("%s:\"%s\"", 
      TestData.PathField, 
      ClientUtils.escapeQueryChars(path))));
    report("Path", r);
  }

  QueryResponse query(SolrQuery q) throws SolrServerException, IOException {
    return dockerServer.getClient().query(TestDockerServer.FileCore, q);
  }
	SolrQuery disJoin(String mainQuery, String[] disJoinQueries) {
    String qs = IntStream.range(0, disJoinQueries.length).mapToObj(i->
      "v" + (i==0?"":i) + "=" + ClientUtils.encodeLocalParamVal(disJoinQueries[i]))
      .collect(Collectors.joining(" "));
		return new SolrQuery(mainQuery).addFilterQuery(String.format(
            "{!disjoin %s}", qs)).setRows(0).setShowDebugInfo(true);
  }
  SolrQuery join(String mainQuery, String joinQuery) {
		return new SolrQuery(mainQuery).addFilterQuery(joinQuery).setRows(0).setShowDebugInfo(true);
  }

  String pathDisJoinQuery(String path, String from, String to) {
		return String.format("%s.%s|%s|%s:%s",
      TestDockerServer.FolderCore, from, to,
      TestData.PathField, ClientUtils.escapeQueryChars(path)
    );
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
    LOGGER.info("Optimizing...");
    SolrClient c = dockerServer.getClient();
    c.optimize(TestDockerServer.FolderCore);
    c.optimize(TestDockerServer.FileCore);
    LOGGER.info("Optimized");
  }
}