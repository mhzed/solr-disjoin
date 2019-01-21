package com.mhzed.solr.disjoin;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrInputDocument;

class TestData {
	static final String PathField = "path_descendent_path";
	static final String IdField = "id";	// for test join by string
	static final String IntField = "id_i";	// for test join by integer
  static final String LongField = "id_l";	// for test join by long
  static final String DoubleField = "id_d";	// for test join by double
  static final String TextField = "id_t"; // for unsuported type test 
	static final String ParentField = "parent_id_s";

  @FunctionalInterface
  interface DocSink {
    void apply(SolrInputDocument doc);
  }
  /**
   * generate test folders breadth first, i.e.
   *    0      1 
   *  2   3  4   5
   * 6 7 8 9 ....
   */
  static List<SolrInputDocument> branch(String path, Integer parentId, int idoffset, 
    int width, int depth) {

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
  static SolrInputDocument docInFolder(Integer folderId) {
    return docOf(
      "folder_" + IdField + "_s", String.valueOf(folderId), 
      "folder_" + IntField, folderId,
      "folder_" + LongField, Long.valueOf(folderId),
      "folder_" + DoubleField, Double.valueOf(folderId),
      "folder_" + TextField, folderId,
      "folder_" + IdField + "_ss", String.valueOf(folderId), 
      "folder_" + IntField + "s", folderId,
      "folder_" + LongField + "s", Long.valueOf(folderId),
      "folder_" + DoubleField + "s", Double.valueOf(folderId),
      "link_folder_" + IdField + "_s", String.valueOf(folderId+1), 
      "link_folder_" + IntField, folderId+1,
      "link_folder_" + LongField, Long.valueOf(folderId+1),
      "type_s", "doc" );
  }
	// generate 1 doc for each folder 
	static List<SolrInputDocument> docsUnder(List<SolrInputDocument> folders) {
		return folders.stream().map(folder-> {
      Integer id = (Integer)folder.getFieldValue(IntField);
			return docInFolder(id);
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