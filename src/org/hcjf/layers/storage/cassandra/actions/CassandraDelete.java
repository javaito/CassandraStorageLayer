package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.*;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraDelete extends Delete<CassandraStorageSession> {

    private static final String UPDATE_STATEMENT = "DELETE * FROM %s WHERE %s";

    public CassandraDelete(CassandraStorageSession session) {
        super(session);
    }

    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        R resultSet = null;
        Select select = getSession().select(getQuery());
        ResultSet selectResultSet = select.execute();

        Map<String, List<Object>> inParameters = new HashMap<>();
        List<String> keys = getSession().getPartitionKey(getSession().normalizeName(getResourceName()));
        keys.addAll(getSession().getClusteringKey(getSession().normalizeName(getResourceName())));

        if(selectResultSet instanceof CollectionResultSet) {
            CollectionResultSet collectionResultSet = (CollectionResultSet) selectResultSet;

        } else {
            resultSet = (R) new EmptyResultSet();
        }

        return resultSet;
    }

}

