package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.cassandra.CassandraStorageSession;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraUpsert extends CassandraInsert {

    public CassandraUpsert(CassandraStorageSession storageSession) {
        super(storageSession);
    }

}
