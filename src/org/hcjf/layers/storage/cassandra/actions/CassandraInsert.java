package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.SingleResult;
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

    private Object addedInstance;

    public CassandraInsert(CassandraStorageSession storageSession) {
        super(storageSession);
    }

    @Override
    protected void onAdd(Object object) {
        setResultType(object.getClass());
        setResourceName(object.getClass().getSimpleName());
        addedInstance = object;
    }

    protected Object getAddedInstance() {
        return addedInstance;
    }

    /**
     *
     * @param <R>
     * @return
     * @throws StorageAccessException
     */
    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        StringBuilder valuesBuilder = new StringBuilder();
        String separator = "";
        StringBuilder valuePlacesBuilder = new StringBuilder();
        List<Object> values = new ArrayList<>();
        String normalizedResourceName = getSession().normalizeName(getResourceName());
        String normalizedStorageValueName;
        Object value;
        for(String storageValueName : getValues().keySet()) {
            normalizedStorageValueName = getSession().normalizeName(storageValueName);
            if(getSession().checkColumn(normalizedResourceName, normalizedStorageValueName)) {
                valuesBuilder.append(separator).append(normalizedStorageValueName);
                valuePlacesBuilder.append(separator).append("?");
                value = getValues().get(storageValueName).getValue();
                if(value != null) {
                    if (value.getClass().isEnum()) {
                        value = value.toString();
                    } else if (value.getClass().equals(Class.class)) {
                        value = ((Class) value).getName();
                    }
                }
                values.add(value);
                separator = ",";
            }
        }

        String cqlStatement = String.format(
                INSERT_STATEMENT, normalizedResourceName, valuesBuilder.toString(), valuePlacesBuilder.toString()).toString();
        ResultSet sessionResultSet = getSession().execute(cqlStatement, values, getResultType());

        R result;
        if(addedInstance != null) {
            result = (R) new SingleResult(addedInstance);
        } else {
            result = (R) sessionResultSet;
        }

        return result;
    }

}
