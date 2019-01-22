# Solr DisJoin

DisJoin stands for disjunction join.

Solr supports a form of [join](https://wiki.apache.org/solr/Join), 
equivalent to RDMBS inner-join on a single field.  Though limited, Join query 
can be very useful in certain contexts.  

Multiple join queries can be combined together by adding each as a filter query 
to the main Solr Query.  The join conditions together form a logical conjunction 
due to the nature of filter query.  However sometimes a disjunction of join 
conditions are desired.  This DisJoin query plugin fills this requirement.  

The examples in the following sections illustrate some use cases for disjunction join.

## Pre-requisites and limitations

Same as Solr's built in [join query](https://lucene.apache.org/solr/guide/7_6/other-parsers.html)

## Installation and usage

Download jar from [releases](https://github.com/mhzed/solr-disjoin/releases).  
Place the jar in the correct Solr directory, see [here](https://wiki.apache.org/solr/SolrPlugins).

In SolrConfig.xml, add:

```xml
<queryParser class="com.mhzed.solr.disjoin.DisJoinQParserPlugin" name="disjoin">
</queryParser>
```

Then construct the following solr query to search:

```java
// one join condition, same as solr built-in join
new SolrQuery("*:*").addFilterQuery("{!disjoin v=fromIndex.id|to_id|title:xyz}");

// the disjunction of two join conditions.
new SolrQuery("*:*").addFilterQuery(
   "{!disjoin v=fromIndex.id|to_id|title:xyz v1=col2.id|to2_id|name:joe}");
```

### Docker

A docker image 'mhzed/solr-disjoin' is prebuilt based on solr:7.6-slim (see Dockerfile). It's essentially a solr image with this plugin pre-installed by default using plugin name "disjoin".

A solr cloud instance with 2 solr nodes can be launched via command:
```
docker-compose -f cloud/docker-compose.yml up
```
Browse to http://localhost:8981/ to access cloud admin console.


## Query format

Multiple join queries are passed in via local parameters "v,v1,v2,...". Make 
sure the index after 'v' is consecutive: i.e. for "v,v1,v3", v3 is dropped. 

When there is exactly one join condition ("v" only), DisJoin behaves the same as solr's 
join query. 

The format of each join query is:
```
(fromCollection.)fromField|toField(,toField)|query
```

* fromCollection: the join from collection name.  If omitted, then it's the same
  as the "to" collection (the collection where this query is being run).  
* fromField: the field in from collection to join on.
* toField: the field in "to" collection to join on.  When there are multiple 
  toFields, then each is joined with 'fromField' and results are combined via
  disjunction.  See example below.
* query: the query to run on fromCollection to collect "fromField" results.


## Example:

In this example there are three collections:  the main "files" collection with N
shards, and "folders" and "types" collection with 1 shard [created via 
colocated collection](https://lucene.apache.org/solr/guide/7_5/colocating-collections.html).
Collection "folder" store a directory structure.  Each file is placed under one folder, but
could also be linked to multiple other parent folders.  Collection "types" stores a 
file type hierarchy linked together via "parent_id".  Each file is associated with one type.

Collection "folder" sample data:
```json
[
  {
    "id"   : "1",
    "path" : "/a"
  },
  {
    "id"   : "2",
    "path" : "/x"
  },
  {
    "id"   : "3",
    "path" : "/a/b"
  }
]
```
* "path" is configured to be of field type "descendant_path".

Collection "types" sample data:
```json
[
  {
    "id" : "1",
    "type" : "root"
  },
  {
    "id" : "2",
    "type" : "manual",
    "parent_id": "1"
  },
  {
    "id" : "3",
    "type" : "user_manual",
    "parent_id" : "2"
  },
  {
    "id" : "4",
    "type" : "review",
    "parent_id" : "1"
  }
]
```

Collection "files" sample data:
```json
[
  {
    "id"  : "17d0dddc-17a6-11e9-ab14-d663bd873d93",
    "folder_id" : "1",
    "linked_folder_ids" : ["2", "3"],
    "type_id" : "1",
    "content" : "..."
  },
  {
    "id"  : "17d0dddc-17a6-11e9-ab14-d663bd873d94",
    "folder_id" : "2",
    "linked_folder_ids" : ["3"],
    "type_id" : "2",
    "content" : "..."
  }
]
```

Using Solr's built-in join query, these queries are possible:

```java 
// find all files with folder_id of folder "/a"
new SolrQuery("*:*").addFilterQuery("{!join fromIndex=folders from=id to=folder_id}path:\\/a");

// find all files with folder_id of folder "/a" or "/x"
new SolrQuery("*:*").addFilterQuery("{!join fromIndex=folders from=id to=folder_id}path:\\/a OR path:\\/x");

// find all files of type "manual" or its descendant types
new SolrQuery("*:*").addFilterQuery("{!join fromIndex=types from=id to=type_id}{!graph from=parent_id to=id}type:manual");
```

The following queries are not possible with "join", but possible with "disjoin": 

```java
// find all files with folder_id or linked_folder_ids of folder "/a" 
new SolrQuery("*:*").addFilterQuery("{!disjoin v=folders.id|folder_id,linked_folder_ids|path:\\/a}")  

// find all files of type "manual" or its descendant types, or under folder "/a"
new SolrQuery("*:*").addFilterQuery("{!disjoin " + 
	"v=folders.id|folder_id|path:\\/a " + 
	"v1=types.id|type_id|{!graph from=parent_id to=id}type:manual}"
	);
```

## Performance test

To run performance test via docker, run:
```
mvn -Dtest=DockerPerformanceTest test
```

Above test will launch a single node solr docker instance, populate it with data, run the join/disjoin queries and produce a report.  The docker instance is named 'solr-disjoin-test', it's preserved locally along with already populated test data.  To remove it, run:
```
docker stop solr-disjoin-test
docker rm solr-disjoin-test
```

On a 2013 MacBook Pro, the sample results are:
```
PathToken(str). Size 5592404 took 2354ms
PathToken(str). Size 1398101 took 1579ms
PathToken(str). Size 1398101 took 1064ms
PathToken(str). Size 1398101 took 868ms
PathToken(str). Size 1398101 took 883ms
PathToken(str). Size 349525 took 662ms
PathToken(str). Size 1365 took 757ms
PathToken(str). Size 5 took 597ms
PathToken(int). Size 5592404 took 18620ms
PathToken(int). Size 1398101 took 39858ms
PathToken(int). Size 1398101 took 6729ms
PathToken(int). Size 1398101 took 53802ms
PathToken(int). Size 1398101 took 9454ms
PathToken(int). Size 349525 took 3693ms
PathToken(int). Size 349525 took 165ms
PathToken(int). Size 349525 took 1097ms
PathToken(int). Size 349525 took 184ms
PathToken(int). Size 87381 took 59ms
PathToken(int). Size 87381 took 49ms
PathToken(int). Size 87381 took 50ms
PathToken(int). Size 1365 took 10ms
PathToken(int). Size 5 took 14ms
PathToken: dis-join of 4 queries. Size 5592404 took 3683ms
Graph. Size 1398101 took 3096ms
Path. Size 1398101 took 89ms
Graph. Size 349525 took 552ms
Path. Size 349525 took 20ms
```

- PathToken(str) joins on a string field
- PathToken(int) joins on a numeric field (LongPoint)
- The benchmark applies to Solr's join query, and DisJoin implementation calls Solr's join query internally
  
Observations:
1. When join on a string field, speed is uniform and proportional to join set size.
2. When join on a numeric (*Point) field, speed is unpredictable.  There could be 20x speed difference on joins with the same set size. Sometimes smaller size took much longer than larger size.  The conjecture here is that perhaps the index layout in memory is causing cache thrashing in some cases.
3. When join on a numeric (*Point) field, if join set size is small (100k or less), speed is very fast in general.
4. Graph query overhead is considerable if result size is in hundreds of thousands or more.  This is perhaps related to "2" as join on *Point fields internally calls Graph query apis.
5. Dis-join of 4 queries is about 20% faster than 4 join queries issued separately.

Conclusions:
* Join on string fields in general.
* If join set size is known to be small, i.e. < 100k, then join on numeric fields is faster.

