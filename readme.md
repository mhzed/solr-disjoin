# Solr DisJoin

DisJoin stands for disjunction join.

Solr supports a limited form of [join](https://wiki.apache.org/solr/Join), equivalent to RDMBS inner-join on a single field.  Though limited, Join query can be very useful in certain contexts []().  

Multiple join queries can be combined together by adding each as a filter query to the main Solr Query.  The join conditions together form a logical conjunction due to the nature of filter query.  However sometimes a disjunction of join conditions are desired.  This DisJoin query plugin fills this requirement.  The examples below illustrate some use cases for disjunction join.

## Pre-requisites and limitations

1. Tested on Solr 7.4
2. The joined fields MUST be configured as 'docValues="true"' in schema.  
3. Current supported field types are: 
   - IntPointField
   - LongPointField
   - DoublePointField
   - StrField

## Configuration and usage

In SolrConfig.xml, add:

    <queryParser class="com.mhzed.solr.disjoin.DisJoinQParserPlugin" name="disjoin">
    </queryParser>

Then construct following solr query:

    // in Java:
    new SolrQuery("*:*").addFilterQuery("{!disjoin v=fromIndex.id|to_id|title:xyz}");

## Example:

Imagine a use case where SOLR is setup to index a file system, where the directory structure is indexed separately from the files to prevent costly re-index of files when the directory structure changes.

In collection "folders", these folder objects are stored:

    {
      "id"   : "1",
      "path" : "/a/b/c"
    }

* "path" is configured to be of field type "descendant_path".

In collection "files", these objects are stored:

    {
      "id"  : "UUIDXXX",
      "folder_id" : "1",
      "content" : "..."
    }


Typically, an application that uses Solr should embed all search-able fields in the document.  Searching is  

    {!disjoin v=folders.id|folder_id|path:"/a"}
