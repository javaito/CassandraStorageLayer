package org.hcjf.layers.storage.cassandra.properties;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import org.hcjf.properties.SystemProperties;

/**
 * @author javaito
 */
public class CassandraProperties {

    public static final String CASSANDRA_STORAGE_LAYER_LOG_TAG = "cassandra.storage.layer.log.tag";
    public static final String CASSANDRA_STORAGE_NAMING_IMPL_NAME = "cassandra.storage.naming.impl.resource";
    public static final String CASSANDRA_STORAGE_NAMING_SEPARATOR = "cassandra.storage.naming.separator";
    public static final String CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_LOCAL_HOST = "cassandra.storage.pool.core.connection.per.local.host";
    public static final String CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_LOCAL_HOST = "cassandra.storage.pool.core.max.connection.per.local.host";
    public static final String CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_REMOTE_HOST = "cassandra.storage.pool.core.connection.per.remote.host";
    public static final String CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_REMOTE_HOST = "cassandra.storage.pool.core.max.connection.per.remote.host";
    public static final String CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_LOCAL_CONNECTION = "cassandra.storage.pool.max.request.per.local.connection";
    public static final String CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_REMOTE_CONNECTION = "cassandra.storage.pool.max.request.per.remote.connection";
    public static final String CASSANDRA_STORAGE_POOL_HEARTBEAT_INTERVAL_SECONDS = "cassandra.storage.pool.heartbeat.interval.seconds";

    public static final class Query {
        public static final String CASSANDRA_STORAGE_LAYER_QUERY_CONSISTENCY_LEVEL = "cassandra.storage.layer.query.consistency.level";
        public static final String CASSANDRA_STORAGE_LAYER_QUERY_DEFAULT_IDEMPOTENCE = "cassandra.storage.layer.query.default.idempotence";
        public static final String CASSANDRA_STORAGE_LAYER_QUERY_FETCH_SIZE = "cassandra.storage.layer.query.fetch.size";
    }

    public static void init(){
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_LAYER_LOG_TAG, "CASSANDRA");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_NAMING_IMPL_NAME, "cassandraNaming");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_NAMING_SEPARATOR, "_");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_LOCAL_HOST, "4");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_LOCAL_HOST, "8");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_REMOTE_HOST, "1");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_REMOTE_HOST, "4");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_LOCAL_CONNECTION, "1024");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_REMOTE_CONNECTION, "512");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_HEARTBEAT_INTERVAL_SECONDS, "30");

        SystemProperties.putDefaultValue(Query.CASSANDRA_STORAGE_LAYER_QUERY_CONSISTENCY_LEVEL, QueryOptions.DEFAULT_CONSISTENCY_LEVEL.toString());
        SystemProperties.putDefaultValue(Query.CASSANDRA_STORAGE_LAYER_QUERY_DEFAULT_IDEMPOTENCE, Boolean.toString(QueryOptions.DEFAULT_IDEMPOTENCE));
        SystemProperties.putDefaultValue(Query.CASSANDRA_STORAGE_LAYER_QUERY_FETCH_SIZE, Integer.toString(QueryOptions.DEFAULT_FETCH_SIZE));
    }

}
