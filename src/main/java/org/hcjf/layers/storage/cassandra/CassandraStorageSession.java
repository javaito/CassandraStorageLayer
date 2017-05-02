package org.hcjf.layers.storage.cassandra;

import com.datastax.driver.core.*;
import org.hcjf.layers.query.JoinableMap;
import org.hcjf.layers.query.Query;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.StorageSession;
import org.hcjf.layers.storage.actions.*;
import org.hcjf.layers.storage.cassandra.actions.CassandraDelete;
import org.hcjf.layers.storage.cassandra.actions.CassandraInsert;
import org.hcjf.layers.storage.cassandra.actions.CassandraSelect;
import org.hcjf.layers.storage.cassandra.actions.CassandraUpdate;
import org.hcjf.layers.storage.cassandra.properties.CassandraProperties;
import org.hcjf.log.Log;
import org.hcjf.names.Naming;
import org.hcjf.properties.*;
import org.hcjf.utils.Introspection;
import org.hcjf.utils.Strings;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class implements the session for the cassandra storage layer implementation.
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraStorageSession extends StorageSession {

    private static final String VALUES_INDEX = "values";
    private static final String KEYS_INDEX = "keys";

    private final Session session;
    private final CassandraStorageLayer layer;

    public CassandraStorageSession(String implName, Session session, CassandraStorageLayer layer) {
        super(implName);
        this.session = session;
        this.layer = layer;
    }

    /**
     * This method execute a query
     * @param query
     * @param cqlStatement
     * @param values
     * @param resultType
     * @param <R>
     * @return
     * @throws StorageAccessException
     */
    public <R extends org.hcjf.layers.storage.actions.ResultSet> R executeQuery(
            Query query, String cqlStatement, List<Object> values, Class resultType) throws StorageAccessException {
        PreparedStatement statement = session.prepare(cqlStatement);

        long totalTime;
        long queryTime = totalTime = System.currentTimeMillis();
        BoundStatement boundStatement = statement.bind(values.toArray());
        com.datastax.driver.core.ResultSet cassandraResultSet =
                    session.execute(boundStatement);
        List<Row> rawRows = cassandraResultSet.all();
        queryTime = System.currentTimeMillis() - queryTime;

        long parsingTime = System.currentTimeMillis();
        Set<Row> rows = query.evaluate(rawRows, new Query.Consumer<Row>() {

            @Override
            public <R> R get(Row row, Query.QueryParameter queryParameter) {
                if(queryParameter instanceof Query.QueryField) {
                    return (R) row.getObject(normalizeName(((Query.QueryField)queryParameter).getFieldName()));
                } else {
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public <R> R resolveFunction(Row o, Query.QueryFunction queryFunction) {
                return null;
            }

        });

        org.hcjf.layers.storage.actions.ResultSet result;
        if(resultType != null) {
            List<Object> instances = new ArrayList<>();
            for (Row row : rows) {
                instances.add(createInstance(resultType, row));
            }

            result = new CollectionResultSet(instances);
        } else {
            List<Map<String, Object>> resultRows = new ArrayList<>();
            for(Row row : rows) {
                resultRows.add(createRows(row, query.getResourceName()));
            }

            result = new MapResultSet(resultRows);
        }
        parsingTime = System.currentTimeMillis() - parsingTime;
        totalTime = System.currentTimeMillis() - totalTime;

        Log.d(SystemProperties.get(CassandraProperties.CASSANDRA_STORAGE_LAYER_LOG_TAG),
                "CQL: %s -> [Query Time: %d ms, Parsing Time: %d ms, Total time: %d ms, Result size: %d]",
                toStringStatement(boundStatement), queryTime, parsingTime, totalTime, rawRows.size());

        try {
            return (R) result;
        } catch (ClassCastException ex) {
            throw new StorageAccessException("", ex);
        }
    }

    /**
     * Creates the string representation of te cql statement.
     * @param statement Cql statement.
     * @return String representation.
     */
    protected String toStringStatement(BoundStatement statement) {
        StringBuilder builder = new StringBuilder(statement.preparedStatement().getQueryString());
        int startIndex;
        for(int i = 0; i < statement.preparedStatement().getVariables().size(); i++) {
            startIndex = builder.indexOf("?");
            builder.replace(startIndex, startIndex + 1, statement.getObject(i) != null ? statement.getObject(i).toString() : "null");
        }
        return builder.toString();
    }

    /**
     * Return the storage layer associated to the storage session.
     * @return Storage layer.
     */
    protected final CassandraStorageLayer getLayer() {
        return layer;
    }

    /**
     * Return the cassandra cluster session instance.
     * @return Cassandra cluster session instance.
     */
    protected final Session getSession() {
        return session;
    }

    /**
     * Return the key space metadata for the associated storage layer.
     * @return Key space metadata.
     */
    protected final KeyspaceMetadata getKeyspaceMetadata() {
        return session.getCluster().getMetadata().getKeyspace(layer.getKeySpace());
    }

    /**
     * Execute the statement over cassandra cluster.
     * @param cqlStatement Cel statement.
     * @param values Statements values.
     * @param resultType Expected result type.
     * @param <R> Expected result set type.
     * @return Storage layer result set instance.
     * @throws StorageAccessException
     */
    public <R extends org.hcjf.layers.storage.actions.ResultSet> R execute(
            String cqlStatement, List<Object> values, Class resultType) throws StorageAccessException {
        PreparedStatement statement = session.prepare(cqlStatement);

        long totalTime;
        long queryTime = totalTime = System.currentTimeMillis();
        BoundStatement boundStatement = statement.bind(values.toArray());
        com.datastax.driver.core.ResultSet cassandraResultSet =
                session.execute(boundStatement);
        queryTime = System.currentTimeMillis() - queryTime;

        long parsingTime = System.currentTimeMillis();
        org.hcjf.layers.storage.actions.ResultSet result;
        if(resultType != null) {
            List<Object> instances = new ArrayList<>();

            for (Row row : cassandraResultSet) {
                instances.add(createInstance(resultType, row));
            }

            if(instances.size() == 0) {
                result = new EmptyResultSet();
            } else if(instances.size() == 1) {
                result = new SingleResult(instances.get(0));
            } else {
                result = new CollectionResultSet(instances);
            }
        } else {
            List<Map<String, Object>> rows = new ArrayList<>();
            Map<String, Object> map;
            for(Row row : cassandraResultSet) {
                map = new HashMap<>();
                for(ColumnDefinitions.Definition definition : row.getColumnDefinitions()) {
                    map.put(normalizeName(definition.getName()), row.getObject(definition.getName()));
                }
                rows.add(map);
            }

            result = new MapResultSet(rows);
        }
        parsingTime = System.currentTimeMillis() - parsingTime;
        totalTime = System.currentTimeMillis() - totalTime;

        Log.d(SystemProperties.get(CassandraProperties.CASSANDRA_STORAGE_LAYER_LOG_TAG),
                "CQL: %s -> [Query Time: %d ms, Parsing Time: %d ms, Total time: %d ms]",
                 toStringStatement(boundStatement), queryTime, parsingTime, totalTime);

        try {
            return (R) result;
        } catch (ClassCastException ex) {
            throw new StorageAccessException("", ex);
        }
    }

    /**
     * This method creates an instance of the result type expected.
     * @param resultType Result type expected
     * @param row Data base row.
     * @return Return an expected instance.
     * @throws StorageAccessException
     */
    protected Object createInstance(Class resultType, Row row) throws StorageAccessException {
        Object instance;
        Object rowValue;
        Introspection.Setter setter;

        try {
            instance = resultType.newInstance();
        } catch (Exception ex) {
            throw new StorageAccessException("Unable to create instance", ex);
        }

        Map<String, Introspection.Setter> setters = Introspection.getSetters(resultType,layer.getNamingImplName());
        for (ColumnDefinitions.Definition definition : row.getColumnDefinitions()) {
            if (setters.containsKey(definition.getName())) {
                try {
                    setter = setters.get(definition.getName());
                    rowValue = row.getObject(definition.getName());
                    if (rowValue != null) {
                        if (setter.getParameterType().isEnum()) {
                            rowValue = Enum.valueOf((Class<? extends Enum>) setter.getParameterType(), (String) rowValue);
                        } else if (setter.getParameterType().equals(Class.class)) {
                            rowValue = Class.forName((String) rowValue);
                        }
                        setter.invoke(instance, rowValue);
                    }
                } catch (Exception ex) {
                    Log.d(SystemProperties.get(CassandraProperties.CASSANDRA_STORAGE_LAYER_LOG_TAG),
                            "Unable to set value", ex);
                }
            }
        }
        return instance;
    }

    /**
     * Create a map from a data base row.
     * @param row Data base row.
     * @return Map with all the values.
     */
    protected JoinableMap createRows(Row row, String resourceName) {
        JoinableMap map = new JoinableMap(resourceName);
        for(ColumnDefinitions.Definition definition : row.getColumnDefinitions()) {
            map.put(normalizeName(definition.getName()), row.getObject(definition.getName()));
        }
        return map;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Verify if the column exist in the resource.
     * @param storageColumn Resource of the data base.
     * @return Return true if the column exist and false if the column not exist.
     */
    public boolean checkColumn(String resourceName, String storageColumn) {
        boolean result = false;
        KeyspaceMetadata keyspaceMetadata =
                session.getCluster().getMetadata().getKeyspace(layer.getKeySpace());
        TableMetadata tableMetadata = keyspaceMetadata.getTable(resourceName);
        for(ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if(columnMetadata.getName().equals(storageColumn)) {
                result = true;
                break;
            }
        }
        return result;
    }

    /**
     * Normalize the names.
     * @param name Name to normalize.
     * @return Normalized name.
     */
    public final String normalizeName(String name) {
        return Naming.normalize(layer.getNamingImplName(), name);
    }

    /**
     * Return the set of partition keys of the table.
     * @param resourceName Table name.
     * @return Set with partition keys.
     */
    public final List<String> getPartitionKey(String resourceName) {
        List<String> result = new ArrayList<>();
        TableMetadata metadata = session.getCluster().getMetadata().
                getKeyspace(layer.getKeySpace()).getTable(resourceName);
        result.addAll(metadata.getPartitionKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
        return result;
    }

    /**
     * Return the set of clustering keys of the table.
     * @param resourceName Table name.
     * @return Set with clustering keys.
     */
    public final List<String> getClusteringKey(String resourceName) {
        List<String> result = new ArrayList<>();
        TableMetadata metadata = session.getCluster().getMetadata().
                getKeyspace(layer.getKeySpace()).getTable(resourceName);
        result.addAll(metadata.getClusteringColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
        return result;
    }

    /**
     * Return a set with indexes of the table.
     * @param resourceName Table name.
     * @return Set with indexes.
     */
    public final List<String> getIndexes(String resourceName) {
        List<String> result = new ArrayList<>();
        TableMetadata metadata = session.getCluster().getMetadata().
                getKeyspace(layer.getKeySpace()).getTable(resourceName);
        String target;
        for(IndexMetadata indexMetadata : metadata.getIndexes()) {
            target = indexMetadata.getTarget();
            if(target.startsWith(VALUES_INDEX)) {
                target = target.replace(VALUES_INDEX, Strings.EMPTY_STRING).
                        replace(Strings.START_GROUP, Strings.EMPTY_STRING).
                        replace(Strings.END_GROUP, Strings.EMPTY_STRING);
            } else if(target.startsWith(KEYS_INDEX)) {
                target = target.replace(KEYS_INDEX, Strings.EMPTY_STRING).
                        replace(Strings.START_GROUP, Strings.EMPTY_STRING).
                        replace(Strings.END_GROUP, Strings.EMPTY_STRING);
            }
            result.add(target);
        }
        return result;
    }

    /**
     * Return the cassandra data type of a column.
     * @param resourceName Resource name.
     * @param columnName Column name.
     * @return Column data type.
     */
    public final DataType getColumnDataType(String resourceName, String columnName) {
        TableMetadata metadata = session.getCluster().getMetadata().
                getKeyspace(layer.getKeySpace()).getTable(resourceName);
        ColumnMetadata columnMetadata = metadata.getColumn(columnName);
        return columnMetadata.getType();
    }

    /**
     * This method check if the value to update is compatible with the cassandra
     * data types.
     * In the case that the value is a collection or map, this method is recursive.
     * @param value Value to check.
     * @return Return the object to be updated.
     */
    public final Object checkUpdateValue(Object value) {
        Object result = value;
        if(result != null) {
            if (result.getClass().isEnum()) {
                result = value.toString();
            } else if (result.getClass().equals(Class.class)) {
                result = ((Class) value).getName();
            } else if (List.class.isAssignableFrom(result.getClass())) {
                List newList = new ArrayList();
                for(Object listValue : ((List)result)) {
                    newList.add(checkUpdateValue(listValue));
                }
                result = newList;
            } else if (Set.class.isAssignableFrom(result.getClass())) {
                Set newSet = new TreeSet();
                for(Object setValue : ((Set)result)) {
                    newSet.add(checkUpdateValue(setValue));
                }
                result = newSet;
            }
        }
        return result;
    }

    /**
     * Return the insert implementation for cassandra storage layer.
     * @return Insert implementation.
     * @throws StorageAccessException
     */
    @Override
    public Insert insert() throws StorageAccessException {
        return new CassandraInsert(this);
    }

    /**
     * Return the insert implementation for cassandra storage layer and set the object to store.
     * @param object Object to store.
     * @return Insert implementation.
     * @throws StorageAccessException
     */
    @Override
    public Insert insert(Object object) throws StorageAccessException {
        Insert insert = insert();
        insert.add(object);
        return insert;
    }

    /**
     * Return the select implementation for cassandra storage layer.
     * @param query Query to create the select instance.
     * @return Select instance.
     * @throws StorageAccessException
     */
    @Override
    public Select select(Query query) throws StorageAccessException {
        CassandraSelect result = new CassandraSelect(this);
        result.setQuery(query);
        return result;
    }

    /**
     * Return the delete implementation for cassandra storage layer.
     * @param instance Instance that will be deleted.
     * @return Delete implementation.
     * @throws StorageAccessException
     */
    @Override
    public Delete delete(Object instance) throws StorageAccessException {
        CassandraDelete delete = new CassandraDelete(this);
        delete.add(instance);
        return delete;
    }

    /**
     * Return the delete implementation for cassandra storage layer.
     * @param query Query to filter the delete operation.
     * @return Delete implementation.
     * @throws StorageAccessException
     */
    @Override
    public Delete delete(Query query) throws StorageAccessException {
        CassandraDelete delete = new CassandraDelete(this);
        delete.setQuery(query);
        return delete;
    }

    /**
     * Return the delete implementation for cassandra storage layer.
     * @return Delete implementation.
     * @throws StorageAccessException
     */
    @Override
    public Delete delete() throws StorageAccessException {
        CassandraDelete delete = new CassandraDelete(this);
        return delete;
    }

    /**
     * Return the update implementation for cassandra storage layer.
     * @return Update implementation.
     * @throws StorageAccessException
     */
    @Override
    public Update update() throws StorageAccessException {
        CassandraUpdate update = new CassandraUpdate(this);
        return update;
    }

    /**
     * Return the update implementation for cassandra storage layer.
     * @param  instance Instance that will be updated.
     * @return Update implementation.
     * @throws StorageAccessException
     */
    @Override
    public Update update(Object instance) throws StorageAccessException {
        CassandraUpdate update = new CassandraUpdate(this);
        update.add(instance);
        return update;
    }

    /**
     * Return the update implementation for cassandra storage layer.
     * @param instance Instance that will be updated.
     * @param values Values to will be updated
     * @return Update implementation.
     * @throws StorageAccessException
     */
    @Override
    public Update update(Object instance, Map<String, Object> values) throws StorageAccessException {
        CassandraUpdate update = new CassandraUpdate(this);
        update.add(instance);
        for(String key : values.keySet()) {
            update.add(key, values.get(key));
        }
        return update;
    }

    /**
     * Return the update implementation for cassandra storage layer.
     * @param query Query to filter the update operation.
     * @return Update implementation.
     * @throws StorageAccessException
     */
    @Override
    public Update update(Query query) throws StorageAccessException {
        CassandraUpdate update = new CassandraUpdate(this);
        update.setQuery(query);
        return update;
    }

    /**
     * Return the update implementation for cassandra storage layer.
     * @param query Query to filter the update operation.
     * @param values Values to will be updated
     * @return Update implementation.
     * @throws StorageAccessException
     */
    @Override
    public Update update(Query query, Map<String, Object> values) throws StorageAccessException {
        Update update = update(query);
        for(String key : values.keySet()) {
            update.add(key, values.get(key));
        }
        return update;
    }
}
