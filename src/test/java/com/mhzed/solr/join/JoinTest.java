package com.mhzed.solr.join;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
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
  
	@Test
	public void test() throws Exception {
		List<SolrInputDocument> folders = branch("", 0, 3, 3);	// size: 3^1 + 3^2 + 3^3 = 39
		new UpdateRequest().add(folders).process(client, SingleFolderCollection);
		client.commit(SingleFolderCollection);
		new UpdateRequest().add(docs(folders)).process(client, DocCollection);
		client.commit(DocCollection);
	
		SolrDocumentList docs = client.query(DocCollection, 
						joinFilterQuery("/0", "id", "folder_id_s", false)).getResults();
		assertEquals(13, docs.size());
	}
	
	SolrQuery joinFilterQuery(String path,
					String from, String to, boolean postFilter) {
		return new SolrQuery("*:*").addFilterQuery(String.format(
						"{!join fromCollection=%s from=%s to=%s pfsz=%s}%s",
						SingleFolderCollection,
						ClientUtils.encodeLocalParamVal(from),
						ClientUtils.encodeLocalParamVal(to),
						postFilter ? 0 : 10000,
						PathField + ":" + ClientUtils.escapeQueryChars(path)
						)).setRows(10000);
	}
	
	// generate test folder docs
	List<SolrInputDocument> branch(String path, int idoffset, int width, int depth) {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		if (depth <= 0) return docs;
		int id = idoffset;
		for (int w=0; w<width; w++) {
			docs.add(docOf(IdField, String.valueOf(id), IntField, id, LongField, id, 
							PathField, path + "/" + w));
			id++;
		}
		for (int w=0; w<width; w++) {
			List<SolrInputDocument> b = branch(path + "/" + w, id, width, depth-1);
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
