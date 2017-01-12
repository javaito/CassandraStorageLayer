package org.hcjf.layers.storage.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.hcjf.layers.storage.StorageLayer;
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
 * @author javaito
 * @mail javaito@gmail.com
 */
public abstract class CassandraStorageLayer extends StorageLayer<CassandraStorageSession> {

    private static final String CASSANDRA_STORAGE_LAYER_LOG_TAG = "CASSANDRA_STORAGE_LAYER";

    public static final String CASSANDRA_CONTACT_POINTS = "cassandra.%s.contact.points";
    public static final String CASSANDRA_KEY_SPACE_NAME = "cassandra.%s.key.space.name";
    public static final String CASSANDRA_USER_NAME = "cassandra.%s.user.name";
    public static final String CASSANDRA_PASSWORD = "cassandra.%s.password";
    public static final String CASSANDRA_NAMING_IMPL = "cassandra.%s.naming.implementation";

    private final Cluster cluster;
    private final Session session;

    public CassandraStorageLayer(String implName) {
        super(implName);
        Naming.addNamingConsumer(new CassandraNaming());
        Formatter formatter = new Formatter();
        SystemProperties.putDefaultValue(
                formatter.format(CASSANDRA_NAMING_IMPL, getImplName()).toString(),
                CassandraNaming.CASSANDRA_NAMING_IMPL);
        cluster = Cluster.builder().addContactPoints(getContactPoints()).build();
        session = cluster.connect(getKeySpace());
    }

    /**
     *
     * @return
     */
    protected CassandraNaming getNamingImpl() {
        return new CassandraNaming();
    }

    /**
     * Return a session with the storage implementation.
     * @return Storage implementation.
     */
    @Override
    public CassandraStorageSession begin() {
        CassandraStorageSession result = new CassandraStorageSession(getImplName(), session, this);
        return result;
    }

    /**
     *
     * @return
     */
    private List<InetAddress> getContactPoints() {
        Formatter formatter = new Formatter();
        List<String> contactPoints = SystemProperties.getList(formatter.format(
                CASSANDRA_CONTACT_POINTS, getImplName()).toString());
        List<InetAddress> result = new ArrayList<>();
        for(String contactPoint : contactPoints) {
            try {
                result.add(InetAddress.getByName(contactPoint));
            } catch (UnknownHostException ex) {
                Log.w(CASSANDRA_STORAGE_LAYER_LOG_TAG, "Unknown host %s", ex, contactPoint);
            }
        }
        return result;
    }

    /**
     *
     * @return
     */
    public final String getKeySpace() {
        Formatter formatter = new Formatter();
        return SystemProperties.get(formatter.format(
                CASSANDRA_KEY_SPACE_NAME, getImplName()).toString());
    }

    /**
     *
     * @return
     */
    private String getUserName() {
        Formatter formatter = new Formatter();
        return SystemProperties.get(formatter.format(
                CASSANDRA_USER_NAME, getImplName()).toString());
    }

    /**
     *
     * @return
     */
    private String getPassword() {
        Formatter formatter = new Formatter();
        return SystemProperties.get(formatter.format(
                CASSANDRA_PASSWORD, getImplName()).toString());
    }

    /**
     *
     * @return
     */
    public String getNamingImplName() {
        Formatter formatter = new Formatter();
        return SystemProperties.get(formatter.format(
                CASSANDRA_NAMING_IMPL, getImplName()).toString());
    }
}
