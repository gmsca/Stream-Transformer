FROM confluentinc/ksqldb-server:0.14.0
COPY join.sql /join/join.sql
ENV KSQL_BOOTSTRAP_SERVERS=broker:29092
ENV KSQL_KSQL_SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV KSQL_KSQL_SERVICE_ID=ksql_standalone_1_
ENV KSQL_KSQL_QUERIES_FILE=/join/join.sql