package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.cassandra.CassandraStorageSession;

/**
 * @author javaito
 */
public class CassandraUpsert extends CassandraInsert {

    public CassandraUpsert(CassandraStorageSession storageSession) {
        super(storageSession);
    }

}
