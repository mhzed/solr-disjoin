# Solr DisJoin

DisJoin stands for disjunction join.

Solr supports a form of [join](https://wiki.apache.org/solr/Join), 
equivalent to RDMBS inner-join on a single field. Multiple join queries can be 
combined together by adding each as a filter query to the main Solr Query. 
Disjunction join can be achieved via the bool query parser plugin.  This project
contains some examples and a performance test on the join query.  

## Disjunction join

DisJoinTest.java contains details.  In short, join queries are passed to a bool 
query as 'should' clauses.

## Performance test

To run performance test via docker, run:
```
mvn -Dtest=DockerPerformanceTest test
```

Above test will launch a single node solr docker instance, populate it with data, run the join/disjoin queries and produce a report.  The docker instance is named 'solr-disjoin-test', it's preserved locally along with already populated test data.  On first run, if the docker image "mhzed/solr-disjoin" needs to be downloaded, it may take a while.  The test also sleeps 10 seconds for the container to start, on some machine it may not be enough, and you will see test error.  In such case, manually start container first:
```
docker start solr-disjoin-test
```
Then run the maven command again.

To remove the test container, run:
```
docker rm solr-disjoin-test
```

On a 2013 MacBook Pro 2.6 GHz Intel Core i5, the sample results are:
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

On another 2 core Intel(R) Xeon(R) Gold 6148 CPU @ 2.40GHz , the sample results are:
```
PathToken(str). Size 5592404 took 2023ms
PathToken(str). Size 1398101 took 1059ms
PathToken(str). Size 1398101 took 804ms
PathToken(str). Size 1398101 took 746ms
PathToken(str). Size 1398101 took 743ms
PathToken(str). Size 349525 took 618ms
PathToken(str). Size 1365 took 605ms
PathToken(str). Size 5 took 541ms
PathToken(int). Size 5592404 took 4979ms
PathToken(int). Size 1398101 took 56891ms
PathToken(int). Size 1398101 took 5848ms
PathToken(int). Size 1398101 took 25612ms
PathToken(int). Size 1398101 took 4246ms
PathToken(int). Size 349525 took 1639ms
PathToken(int). Size 349525 took 77ms
PathToken(int). Size 349525 took 89ms
PathToken(int). Size 349525 took 151ms
PathToken(int). Size 87381 took 27ms
PathToken(int). Size 87381 took 21ms
PathToken(int). Size 87381 took 223ms
PathToken(int). Size 1365 took 5ms
PathToken(int). Size 5 took 7ms
PathToken: dis-join of 4 queries. Size 5592404 took 3071ms
Graph. Size 1398101 took 2479ms
Path. Size 1398101 took 41ms
Graph. Size 349525 took 414ms
Path. Size 349525 took 6ms
```

- PathToken(str) joins on a string field
- PathToken(int) joins on a numeric field (LongPoint)
- The benchmark applies to Solr's join query, as DisJoin implementation calls Solr's join query internally
  
Observations:
1. When join on a string field, speed is uniform and proportional to join set size.
2. When join on a numeric (*Point) field, speed is unpredictable.  There could be 20x speed difference on joins with the same set size. Sometimes smaller size took much longer than larger size.  The conjecture here is that perhaps the index layout in memory is causing cache thrashing in some cases.
3. When join on a numeric (*Point) field, if join set size is small (100k or less), speed is very fast in general.
4. Graph query overhead is considerable if result size is in hundreds of thousands or more.  This is perhaps related to "2" as join on *Point fields internally calls Graph query apis.
5. Dis-join of 4 queries is about 20% faster than 4 join queries issued separately.

Conclusions:
* Join on string fields in general.
* If join set size is known to be small, i.e. < 100k, then join on numeric fields is faster.

