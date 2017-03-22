package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.SingleResult;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;

import java.util.*;

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
                value = checkValue(getValues().get(storageValueName).getValue());
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

    protected Object checkValue(Object value) {
        Object result = value;
        if(result != null) {
            if (result.getClass().isEnum()) {
                result = value.toString();
            } else if (result.getClass().equals(Class.class)) {
                result = ((Class) value).getName();
            } else if (List.class.isAssignableFrom(result.getClass())) {
                List newList = new ArrayList();
                for(Object listValue : ((List)result)) {
                    newList.add(checkValue(listValue));
                }
                result = newList;
            } else if (Set.class.isAssignableFrom(result.getClass())) {
                Set newSet = new TreeSet();
                for(Object setValue : ((Set)result)) {
                    newSet.add(checkValue(setValue));
                }
                result = newSet;
            }
        }
        return result;
    }

}
