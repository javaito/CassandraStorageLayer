package org.hcjf.main;

import com.datastax.driver.core.utils.UUIDs;
import org.hcjf.layers.Layers;
import org.hcjf.layers.storage.StorageLayerInterface;
import org.hcjf.layers.storage.StorageSession;
import org.hcjf.layers.storage.actions.CollectionResultSet;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.cassandra.CassandraStorageLayer;

import java.util.Date;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class Main {

    public static void main(String[] args) throws Exception {
        System.setProperty("cassandra.main.contact.points", "[localhost]");
        System.setProperty("cassandra.main.key.space.name", "test");

        Layers.publishLayer(TestStorageLayer.class);

        Auto auto = new Auto();
        auto.setId(UUIDs.timeBased());
        auto.setName("Nuevo auto de testing " + System.currentTimeMillis());
        auto.setLastUpdate(new Date());

        StorageLayerInterface<?> storageLayerInterface =
                Layers.get(StorageLayerInterface.class, "main");
        try (StorageSession session = storageLayerInterface.begin();) {
            Insert<?> insert = session.insert();
            insert.add(auto);
            CollectionResultSet rs = insert.execute();
        }
//        Auto auto2 = rs.getResult();
//
//        System.out.println(auto2);
    }

}
