package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.StorageLayer;
import org.hcjf.layers.storage.actions.Insert;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.SingleResult;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Introspection;
import org.hcjf.utils.Strings;

import java.util.*;

/**
 * This class implements the insert operation for cassandra data base.
 * @author javaito
 */
public class CassandraInsert extends Insert<CassandraStorageSession> {

    private static final String INSERT_STATEMENT = "INSERT INTO %s (%s) VALUES (%s) IF NOT EXISTS;";

    private Object addedInstance;

    public CassandraInsert(CassandraStorageSession storageSession) {
        super(storageSession);
    }

    @Override
    protected void onAdd(Object object) {
        setResultType(object.getClass());
        setResourceName(object.getClass().getSimpleName());
        for(Introspection.Getter getter : Introspection.getGetters(object.getClass()).values()) {
            try {
                add(getter.getResourceName(), new FieldStorageValue(getter.get(object), getter.getAnnotationsMap()));
            } catch(Exception ex) {
                Log.w(StorageLayer.STORAGE_LOG_TAG, "Invoke getter method fail: $s", ex, getter.getResourceName());
            }
        }
        addedInstance = object;
    }

    protected Object getAddedInstance() {
        return addedInstance;
    }

    /**
     * This method create the insert statement for the cassandra data base an execute the statement.
     * @param <R> Expected result set.
     * @return Return the inserted objects.
     * @throws StorageAccessException StorageAccessException
     */
    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        //Builder to create 'values' body for insert statement.
        Strings.Builder valuesBuilder = new Strings.Builder();
        //Builder to create paces body for insert statement.
        Strings.Builder valuePlacesBuilder = new Strings.Builder();
        //List of values to execute the insert statement
        List<Object> values = new ArrayList<>();

        String normalizedResourceName = getSession().normalizeName(getResourceName());
        String normalizedStorageValueName;
        for(String storageValueName : getValues().keySet()) {
            normalizedStorageValueName = getSession().normalizeName(storageValueName);
            if(getSession().checkColumn(normalizedResourceName, normalizedStorageValueName)) {
                valuesBuilder.append(normalizedStorageValueName, Strings.ARGUMENT_SEPARATOR);
                valuePlacesBuilder.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE), Strings.ARGUMENT_SEPARATOR);
                values.add(getSession().checkUpdateValue(getValues().get(storageValueName).getValue()));
            }
        }

        String statement = String.format(
                INSERT_STATEMENT, normalizedResourceName, valuesBuilder.toString(), valuePlacesBuilder.toString());
        ResultSet sessionResultSet = getSession().execute(statement, values, getResultType());

        R result;
        if(addedInstance != null) {
            result = (R) new SingleResult(addedInstance);
        } else {
            result = (R) sessionResultSet;
        }

        return result;
    }

}
