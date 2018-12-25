package com.mhzed.solr.disjoin;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.mhzed.solr.disjoin.dv.DocValReader;
import com.mhzed.solr.disjoin.dv.DvSetCollector;

import org.apache.lucene.search.Query;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.RefCounted;

class JoinQstr {
  final String fromIndex;
  final String fromField;
  final List<String> toFields;
  final Query q;
  final long fromCoreCommitTime;

  // these members do not participate in equals() and hashCode()
  final String query;  
  final FieldType toFieldType;
  final SolrCore fromCore;

  private JoinQstr(String fromIndex, String fromField, List<String> toFields, String query,
    Query q, long fromCoreCommitTime,  FieldType toFieldType, SolrCore fromCore) {
    this.fromIndex = fromIndex;
    this.fromField = fromField;
    this.toFields = toFields;
    this.query = query;
    this.q = q;
    this.fromCoreCommitTime = fromCoreCommitTime;
    this.toFieldType = toFieldType;
    this.fromCore = fromCore;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj != null && getClass() == obj.getClass())) {
      return false;
    }
    JoinQstr other = getClass().cast(obj);
    return this.fromField.equals(other.fromField) && this.toFields.equals(other.toFields) && this.q.equals(other.q)
        && fromIndex.equals(other.fromIndex) && fromCoreCommitTime == other.fromCoreCommitTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromField, toFields, q, fromIndex, fromCoreCommitTime);
  }

  static final String regField = "[\\pL\\pN_]+";
  static final String regFields = "[\\pL\\pN_,]+";
  // legal collection name regex: see SolrIdentifierValidator.identifierPattern
  static final String regCol = "(?!\\-)[\\._A-Za-z0-9\\-]+";
  static final Pattern vPattern = Pattern
      .compile(String.format("^(?:(%s)\\.)?(%s)\\|(%s)\\|(.*)$", regCol, regField, regFields));

  public static JoinQstr parse(String v) throws SyntaxError {
    Matcher m = vPattern.matcher(v);
    if (m.find()) {
      return new JoinQstr(m.group(1), m.group(2), Arrays.asList(m.group(3).split(",")), m.group(4), null, 0, null, null);
    } else {
      throw new SyntaxError("Invalid syntax: " + v);
    }
  }

  public static JoinQstr create(String v, SolrQueryRequest req, QParser qParser) throws SyntaxError {
    JoinQstr r = parse(v);
    List<FieldType> toTypes = r.toFields.stream().map(f->req.getSchema().getField(f).getType())
        .distinct().collect(Collectors.toList());
    if (toTypes.size() > 1) {
      throw new IllegalArgumentException(String.format("Join toField types are not of the same type: %s.%s",
        r.fromIndex, r.toFields.stream().collect(Collectors.joining(","))
      ));
    }
    FieldType toFieldType = toTypes.get(0);
    final String fromIndex = r.fromIndex == null ? 
      req.getCore().getCoreDescriptor().getCollectionName() : r.fromIndex;
    long fromCoreCommitTime = 0;
    Query q = null;

    SolrCore fromCore = (req.getCore().getCoreDescriptor().getCollectionName().equalsIgnoreCase(fromIndex)) ? req.getCore()
        : req.getCore().getCoreContainer().getCores().stream()
            .filter((c) -> c.getCoreDescriptor().getCollectionName().equalsIgnoreCase(fromIndex)).findFirst().get();

    if (fromCore != req.getCore()) {
      RefCounted<SolrIndexSearcher> s = fromCore.getRegisteredSearcher();
      if (s != null) {
        fromCoreCommitTime = s.get().getOpenNanoTime();
        LocalSolrQueryRequest oreq = new LocalSolrQueryRequest(fromCore, qParser.getParams());
        try {
          QParser parser = QParser.getParser(r.query, oreq);
          q = parser.getQuery();
        } finally {
          oreq.close();
          s.decref();
        }
      }
    } else { // same core
      QParser fromQueryParser = qParser.subQuery(r.query, null);
      fromQueryParser.setIsFilter(true);
      q = fromQueryParser.getQuery();
    }
    return new JoinQstr(fromIndex, r.fromField, r.toFields, r.query, 
      q, fromCoreCommitTime, toFieldType, fromCore);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Set<?> exec() {
    fromCore.open(); // add ref count
    RefCounted<SolrIndexSearcher> fromRef = null;
    try {
      fromRef = fromCore.getSearcher(false, true, null);
      SolrIndexSearcher fromSearcher = fromRef.get();
      DocValReader<?> dvr = DisJoinQueryUtil.getDocValReader(fromField,
          DisJoinQueryUtil.parseType(fromSearcher.getSchema().getField(fromField).getType()));
      Set<Object> results = new HashSet<Object>();
      fromSearcher.search(this.q, new DvSetCollector(dvr, results));
      return results;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (fromRef != null)
        fromRef.decref();
      fromCore.close(); // dec ref count
    }
  }

}
