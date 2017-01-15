package org.hcjf.layers.storage.cassandra.properties;

import org.hcjf.properties.SystemProperties;

/**
 * @author javaito
 * @email javaito@gmail.com
 */
public class CassandraProperties {

    public static final String CASSANDRA_STORAGE_NAMING_IMPL_NAME = "cassandra.storage.naming.impl.resource";
    public static final String CASSANDRA_STORAGE_NAMING_SEPARATOR = "cassandra.storage.naming.separator";
    public static final String CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_LOCAL_HOST = "cassandra.storage.pool.core.connection.per.local.host";
    public static final String CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_LOCAL_HOST = "cassandra.storage.pool.core.max.connection.per.local.host";
    public static final String CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_REMOTE_HOST = "cassandra.storage.pool.core.connection.per.remote.host";
    public static final String CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_REMOTE_HOST = "cassandra.storage.pool.core.max.connection.per.remote.host";
    public static final String CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_LOCAL_CONNECTION = "cassandra.storage.pool.max.request.per.local.connection";
    public static final String CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_REMOTE_CONNECTION = "cassandra.storage.pool.max.request.per.remote.connection";
    public static final String CASSANDRA_STORAGE_POOL_HEARTBEAT_INTERVAL_SECONDS = "cassandra.storage.pool.heartbeat.interval.seconds";

    public static void init(){
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_NAMING_IMPL_NAME, "cassandraNaming");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_NAMING_SEPARATOR, "_");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_LOCAL_HOST, "4");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_LOCAL_HOST, "8");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_REMOTE_HOST, "1");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_REMOTE_HOST, "4");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_LOCAL_CONNECTION, "1024");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_REMOTE_CONNECTION, "512");
        SystemProperties.putDefaultValue(CASSANDRA_STORAGE_POOL_HEARTBEAT_INTERVAL_SECONDS, "30");
    }

}
