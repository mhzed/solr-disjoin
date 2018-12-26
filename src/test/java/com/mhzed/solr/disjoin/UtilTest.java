package com.mhzed.solr.disjoin;
import static org.junit.Assert.assertEquals;

import org.apache.solr.search.SyntaxError;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UtilTest {
	@BeforeClass
	public static void setup() throws Exception {
  }
  @AfterClass
  public static void teardown() throws Exception {
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

    assertEquals(p3.equals(null), false);
    assertEquals(p3.equals((Object)""), false);
  }
  @Test(expected = SyntaxError.class)
  public void testQstrParserError() throws SyntaxError {
    JoinQstr.parse("fol.ders.id|toid");
  }

}