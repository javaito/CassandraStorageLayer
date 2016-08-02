package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.query.Query;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraSelect extends Select<CassandraStorageSession> {

    public CassandraSelect(CassandraStorageSession session) {
        super(session);
    }

    /**
     * @return
     */
    @Override
    public <R extends ResultSet> R execute() throws StorageAccessException {
        Query query = getQuery();




        return null;
    }
}
