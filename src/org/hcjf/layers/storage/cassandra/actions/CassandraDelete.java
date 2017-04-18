package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.*;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;
import org.hcjf.layers.storage.cassandra.properties.CassandraProperties;
import org.hcjf.log.Log;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Introspection;
import org.hcjf.utils.Strings;

import java.util.*;

/**
 * This class implements the delete operation for cassandra.
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraDelete extends Delete<CassandraStorageSession> {

    private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE %s";

    private final List<Object> deleteInstances;

    public CassandraDelete(CassandraStorageSession session) {
        super(session);
        deleteInstances = new ArrayList<>();
    }

    /**
     * Store the added object for gonna be deleted.
     * @param object Added object.
     */
    @Override
    protected void onAdd(Object object) {
        deleteInstances.add(object);
    }

    /**
     * This method first execute a query to found all the rows to delete then creates a single
     * delete operation foreach row and execute this operations.
     * @param params Query parameters
     * @param <R> Expected result set.
     * @return Return the result set with all the rows deleted.
     * @throws StorageAccessException
     */
    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        R resultSet;

        String resourceName;
        if(getResultType() != null) {
            resourceName = getSession().normalizeName(getResultType().getSimpleName());
        } else if(getResourceName() != null) {
            resourceName = getSession().normalizeName(getResourceName());
        } else {
            throw new StorageAccessException("Resource name not found");
        }

        //Obtains the partitions keys and clustering keys for the specific resource.
        List<String> keys = getSession().getPartitionKey(getSession().normalizeName(resourceName));
        keys.addAll(getSession().getClusteringKey(getSession().normalizeName(resourceName)));

        //Creates the base statement for all the deletes.
        Strings.Builder whereBuilder = new Strings.Builder();
        for(String key : keys) {
            whereBuilder.append(key).append(Strings.WHITE_SPACE).append(SystemProperties.get(SystemProperties.Query.ReservedWord.EQUALS));
            whereBuilder.append(Strings.WHITE_SPACE).append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE));
            whereBuilder.append(Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
        }

        //Creates statement string
        String statement = String.format(DELETE_STATEMENT, resourceName, whereBuilder.toString());

        //Creates a list to store the values for the delete statement.
        List<Object> values = new ArrayList<>();

        //These collections are for store the deleted objects.
        List<Object> resultCollection = getResultType() != null ? new ArrayList<>() : null;
        List<Map<String, Object>> resultMap = getResultType() == null ? new ArrayList<>() : null;

        if(!deleteInstances.isEmpty()) {
            for(Object deleteInstance : deleteInstances) {
                try {
                    //The values list is cleared for each row then put in the list the current row values.
                    values.clear();

                    Map<String, Introspection.Getter> instanceGetters = Introspection.getGetters(deleteInstance.getClass());
                    for (String key : keys) {
                        values.add(instanceGetters.get(getSession().normalizeName(key)).get(deleteInstance));
                    }

                    //Execute the delete statement.
                    getSession().execute(statement, values, getResultType());

                    resultCollection.add(deleteInstance);
                } catch (Exception ex) {
                    Log.w(SystemProperties.get(CassandraProperties.CASSADNRA_STORAGE_LAYER_LOG_TAG),
                            "Unable to delete instance %s", deleteInstance.toString());
                }
            }
        } else if(getQuery() != null) {
            //Make cassandra select
            Select select = getSession().select(getQuery());
            MapResultSet selectResultSet = (MapResultSet) select.execute(params);

            for (Map<String, Object> row : selectResultSet.getResult()) {
                try {
                    //The values list is cleared for each row then put in the list the current row values.
                    values.clear();
                    for (String key : keys) {
                        values.add(row.get(getSession().normalizeName(key)));
                    }

                    //Execute the delete statement.
                    getSession().execute(statement, values, getResultType());

                    //If the expected type is a specific object then creates an instance foreach row and put it into
                    //the result list.
                    if (getResultType() != null) {
                        resultCollection.add(Introspection.toInstance(row, getResultType()));
                    } else {
                        resultMap.add(row);
                    }
                } catch (Exception ex){
                    Log.w(SystemProperties.get(CassandraProperties.CASSADNRA_STORAGE_LAYER_LOG_TAG),
                            "Unable to delete row %s", row.toString());
                }
            }
        } else {
            throw new IllegalArgumentException("To delete information you must specify a query or instance to delete");
        }

        if(getResultType() != null) {
            resultSet = (R) new CollectionResultSet(resultCollection);
        } else {
            resultSet = (R) new MapResultSet(resultMap);
        }

        return resultSet;
    }

}

