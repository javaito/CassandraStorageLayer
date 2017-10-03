package org.hcjf.layers.storage.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import org.hcjf.layers.storage.StorageLayer;
import org.hcjf.layers.storage.cassandra.properties.CassandraProperties;
import org.hcjf.log.Log;
import org.hcjf.names.CassandraNaming;
import org.hcjf.names.Naming;
import org.hcjf.properties.SystemProperties;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

/**
 * This class implements the storage layer to work with cassandra data base.
 * @author javaito
 */
public abstract class CassandraStorageLayer<S extends CassandraStorageSession> extends StorageLayer<S> {

    private Cluster cluster;
    private Session session;

    public CassandraStorageLayer(String implName) {
        super(implName);
        Naming.addNamingConsumer(new CassandraNaming());
    }

    /**
     * Return a session with the storage implementation.
     * @return Storage implementation.
     */
    @Override
    public S begin() {
        synchronized (this) {
            if(cluster == null) {
                createCluster();
                session = cluster.connect(getKeySpace());
            }
        }

        return createSessionInstance(getImplName());
    }

    /**
     * This method must be implemented to indicate which implementation of
     * cassandra session will be use the storage layer implementation.
     * @param implName Implementation name.
     * @return Cassandra session implementation instance.
     */
    protected abstract S createSessionInstance(String implName);

    /**
     * Creates a cassandra cluster client.
     */
    private synchronized void createCluster() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setCoreConnectionsPerHost(HostDistance.LOCAL,
                        SystemProperties.getInteger(CassandraProperties.CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_LOCAL_HOST))
                .setMaxConnectionsPerHost(HostDistance.LOCAL,
                        SystemProperties.getInteger(CassandraProperties.CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_LOCAL_HOST))
                .setCoreConnectionsPerHost(HostDistance.REMOTE,
                        SystemProperties.getInteger(CassandraProperties.CASSANDRA_STORAGE_POOL_CORE_CONNECTION_PER_REMOTE_HOST))
                .setMaxConnectionsPerHost(HostDistance.REMOTE,
                        SystemProperties.getInteger(CassandraProperties.CASSANDRA_STORAGE_POOL_CORE_MAX_CONNECTION_PER_REMOTE_HOST))
                .setMaxRequestsPerConnection(HostDistance.LOCAL,
                        SystemProperties.getInteger(CassandraProperties.CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_LOCAL_CONNECTION))
                .setMaxRequestsPerConnection(HostDistance.REMOTE,
                        SystemProperties.getInteger(CassandraProperties.CASSANDRA_STORAGE_POOL_MAX_REQUEST_PER_REMOTE_CONNECTION))
                .setHeartbeatIntervalSeconds(SystemProperties.getInteger(
                        CassandraProperties.CASSANDRA_STORAGE_POOL_HEARTBEAT_INTERVAL_SECONDS));

        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(getContactPoints());
        builder.withCredentials(getUserName(), getPassword());
        builder.withClusterName(getClusterName());
        builder.withLoadBalancingPolicy(getLoadBalancingPolicy());
        builder.withReconnectionPolicy(getReconnectionPolicy());
        builder.withPoolingOptions(poolingOptions);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions
                .setDefaultIdempotence(SystemProperties.getBoolean(
                        CassandraProperties.Query.CASSANDRA_STORAGE_LAYER_QUERY_DEFAULT_IDEMPOTENCE))
                .setConsistencyLevel(ConsistencyLevel.valueOf(SystemProperties.get(
                        CassandraProperties.Query.CASSANDRA_STORAGE_LAYER_QUERY_CONSISTENCY_LEVEL)))
                .setFetchSize(SystemProperties.getInteger(
                        CassandraProperties.Query.CASSANDRA_STORAGE_LAYER_QUERY_FETCH_SIZE));

        builder.withQueryOptions(queryOptions);

        cluster = builder.build();
    }

    /**
     * Creates and returns a cassandra session instance.
     * @return Cassandra session instance.
     */
    protected synchronized Session getCassandraSession() {
        if(cluster.isClosed()) {
            try {
                cluster.close();
            } catch (Exception ex){}
            createCluster();
        }
        if(session == null || session.isClosed()) {
            session = cluster.connect(getKeySpace());
        }
        return session;
    }

    /**
     * Return a list with all the contact points to connect the
     * client with the cassandra cluster.
     * @return List with all the contact points.
     */
    protected abstract List<InetAddress> getContactPoints();

    /**
     * Return the key space resource that will use to connect with the cluster
     * @return Key space resource.
     */
    protected abstract String getKeySpace();

    /**
     * Return the user resource that will use to connect with the cluster.
     * @return User resource.
     */
    protected abstract String getUserName();

    /**
     * Return the password that will use to connect with the cluster.
     * @return Password.
     */
    protected abstract String getPassword();

    /**
     * Return the naming implementation to normalize the connection names.
     * @return Naming implementation resource.
     */
    public String getNamingImplName() {
        return CassandraNaming.CASSANDRA_NAMING_IMPL;
    }

    /**
     * Return the cluster resource that will use to connect with the cluster.
     * @return Cluster resource.
     */
    protected abstract String getClusterName();

    /**
     * Return the balance policy that will use to connect with the cluster.
     * @return Balance policy.
     */
    protected LoadBalancingPolicy getLoadBalancingPolicy() {
        return Policies.defaultLoadBalancingPolicy();
    }

    /**
     * Return the reconnection policy that will use to connect with the cluster.
     * @return Reconnection policy.
     */
    protected ReconnectionPolicy getReconnectionPolicy() {
        return Policies.defaultReconnectionPolicy();
    }
}
