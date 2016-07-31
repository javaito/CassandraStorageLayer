package org.hcjf.main;

import org.hcjf.layers.storage.cassandra.CassandraStorageLayer;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class TestStorageLayer extends CassandraStorageLayer {

    public TestStorageLayer() {
        super("main");
    }

}
