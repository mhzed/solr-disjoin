# Solr Filter Join

This project adds support for SQL-esque "Equi Join" query to SOLR via solr.FilterQuery or solr.PostFilter.

Sometime a client may want to search with fields that are spread over more than one Solr collections.  For example:

1. Search on collection 1, return a set of values
2. Search on collection 2, filter the results by checking if a result is included in the set of values returned by 1.

Doing 1+2 in the client is in-efficient, needless to say.  This project provides a solr.QParserPlugin that performs the join in the Solr JVM using the most efficient mechanisms possible:  solr.FilterQuery, or solr.PostFilter (if the join set is large).

## Pre-requisites and limitations

1. The only join condition supported and assumed is single field equality.
2. The joined field MUST be configured as 'docValues="true"' in schema.  

## Setup

In solrconfig.xml, add

## Search


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


Typical usage:
{!join collection=folders query=path:/a/b/c on=id:folder_id}



The 