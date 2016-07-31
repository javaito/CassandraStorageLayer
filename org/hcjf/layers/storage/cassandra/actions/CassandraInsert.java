package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraInsert extends Insert<CassandraStorageSession> {

    private static final String INSERT_STATEMENT = "INSERT INTO %s (%s) VALUES (%s);";

    private final Formatter formatter;

    public CassandraInsert(CassandraStorageSession storageSession) {
        super(storageSession);
        this.formatter = new Formatter();
    }

    /**
     *
     * @param <R>
     * @return
     * @throws StorageAccessException
     */
    @Override
    public <R extends ResultSet> R execute() throws StorageAccessException {
        StringBuilder valuesBuilder = new StringBuilder();
        String separator = "";
        StringBuilder valuePlacesBuilder = new StringBuilder();
        List<Object> values = new ArrayList<>();
        String normalizedResourceName = getSession().normalizeName(getResourceName());
        String normalizedStorageValueName;
        for(String storageValueName : getValues().keySet()) {
            normalizedStorageValueName = getSession().normalizeName(storageValueName);
            if(getSession().checkColumn(normalizedResourceName, normalizedStorageValueName)) {
                valuesBuilder.append(separator).append(normalizedStorageValueName);
                valuePlacesBuilder.append(separator).append("?");
                values.add(getValues().get(storageValueName).getValue());
                separator = ",";
            }
        }

        String cqlStatement = formatter.format(
                INSERT_STATEMENT, normalizedResourceName, valuesBuilder.toString(), valuePlacesBuilder.toString()).toString();
        return getSession().execute(cqlStatement, values, getResultType());
    }

}
