package org.hcjf.main;

import com.datastax.driver.core.utils.UUIDs;
import org.hcjf.layers.Layers;
import org.hcjf.layers.query.Equals;
import org.hcjf.layers.query.Like;
import org.hcjf.layers.query.Query;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.StorageLayerInterface;
import org.hcjf.layers.storage.StorageSession;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.actions.SingleResult;

import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;

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
        SingleResult rs;
        try (StorageSession session = storageLayerInterface.begin()) {
            Insert<?> insert = session.insert();
            insert.add(auto);
            rs = insert.execute();
        }
        Auto auto2 = rs.getResult();
        System.out.println(auto2);

        Query query = new Query();
//        query.addEvaluator(new Equals("id", UUID.fromString("813824d0-7f8f-11e6-a3ab-e98d1ee5b31f")));
        query.addEvaluator(new Like("name", Pattern.compile(".*9.*2.*")));

        ResultSet resultSet;
        try (StorageSession session = storageLayerInterface.begin()) {
            Select<?> select = session.select(query);
            select.add(new Auto());
            resultSet = select.execute();
        }
    }

}
