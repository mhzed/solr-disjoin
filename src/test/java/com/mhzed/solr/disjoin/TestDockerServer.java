package com.mhzed.solr.disjoin;

import java.io.IOException;
import java.util.Arrays;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;


public class TestDockerServer extends TestServer {
  public static final String ContainerName = "solr-disjoin-test";
  public static final int TestPort = 26453;
  public static final String FileCore = "files";
  public static final String FolderCore = "folders";
  @Override
  public Config config() throws Exception {
    Config c = new Config(ContainerName, Arrays.asList("docker", "run", "--name", 
      ContainerName, "-p", String.format("%d:8983", TestPort), "-t", "mhzed/solr-disjoin"));
		c.port = TestPort;
    return c;
  }
  @Override
  public void launch() throws Exception {
    Process p = exec(new Config("start docker", Arrays.asList("docker", "start", ContainerName) ));
    try {
      waitForPort(5, 2000, TestPort, p);
    } catch (Exception e) {
      super.launch();
    }
    client = new HttpSolrClient.Builder(getSolrUrl()).build();    
  }
  @Override
  public void shutdown() throws Exception {
    super.shutdown();
    exec(new Config("stop docker", Arrays.asList("docker", "stop", ContainerName) ));
  }
  public String getSolrUrl() {
		return String.format("http://localhost:%d/solr", TestPort);
  }
  private SolrClient client = null;
  public SolrClient getClient() {
    return client;
  }
  public CoreAdminResponse waitCoreStatus() throws Exception {
    Thread.sleep(10000);
    CoreAdminRequest adminReq = new CoreAdminRequest();
    adminReq.setAction(CoreAdminAction.STATUS);
    for (int n=0; n<20; n++) {
      try {
        CoreAdminResponse r = adminReq.process(client);
        if (checkAllLoaded(r)) return r;
        else Thread.sleep(1000);
      } catch (Exception e) {
        Thread.sleep(1000);
      }
    }
    throw new Exception("Wait timed out");
  }
  public void ensureCoresCreated() throws Exception {
    CoreAdminResponse r = waitCoreStatus();
    if (!checkCoreExists(r, FolderCore)) {
      createCore(FolderCore);
    }
    if (!checkCoreExists(r, FileCore)) {
      createCore(FileCore);
    }
  }
  private CoreAdminResponse createCore(String name) throws SolrServerException, IOException {
    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName(name);
    req.setConfigSet("_default");
    return req.process(client);
  }
  private boolean checkCoreExists(CoreAdminResponse r, String name) {
    for (int i = 0; i < r.getCoreStatus().size(); i++) {
      NamedList<Object> coreInfo = r.getCoreStatus().getVal(i);
      if (coreInfo.get("name").toString().equals(name)) return true;
    }
    return false;
  }
  private boolean checkAllLoaded(CoreAdminResponse r) {

    for (int i = 0; i < r.getCoreStatus().size(); i++) {
      NamedList<Object> coreInfo = r.getCoreStatus().getVal(i);
      if (coreInfo.get("startTime") == null && coreInfo.getBooleanArg("isLoaded").booleanValue() == false) {
        return false;
      }
    }
    return true;
  }

}