package com.mhzed.solr.disjoin;

import org.apache.solr.search.SyntaxError;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DockerPerformanceTest {
  static TestDockerServer dockerServer;
	@BeforeClass
	public static void setup() throws Exception {
    dockerServer = new TestDockerServer();
    dockerServer.launch();
    dockerServer.ensureCoresCreated();
  }
  @AfterClass
  public static void teardown() throws Exception {
    dockerServer.shutdown();
  }	
  @Test
  public void test() throws SyntaxError {

  }
}