package com.mhzed.solr.disjoin;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.search.SyntaxError;

class JoinQstr {
  final String fromIndex;
  final String fromField;
  final List<String> toFields;
  final String query;  

  private JoinQstr(String fromIndex, String fromField, List<String> toFields, String query) {
    this.fromIndex = fromIndex;
    this.fromField = fromField;
    this.toFields = toFields;
    this.query = query;
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
      return new JoinQstr(m.group(1), m.group(2), Arrays.asList(m.group(3).split(",")), m.group(4));
    } else {
      throw new SyntaxError("Invalid syntax: " + v);
    }
  }
}
