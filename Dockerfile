FROM solr:7.6-slim
COPY target/solr-disjoin-1.0.0.jar /opt/solr/server/solr/lib
RUN sed -i "s|</config>|<queryParser class=\"com.mhzed.solr.disjoin.DisJoinQParserPlugin\" name=\"disjoin\" />\n</config>|g" /opt/solr/server/solr/configsets/_default/conf/solrconfig.xml
WORKDIR /opt/solr
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["solr-foreground"]
