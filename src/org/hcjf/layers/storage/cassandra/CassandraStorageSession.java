package org.hcjf.layers.storage.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import org.hcjf.layers.query.Query;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.StorageSession;
import org.hcjf.layers.storage.actions.*;
import org.hcjf.layers.storage.cassandra.actions.CassandraInsert;
import org.hcjf.layers.storage.cassandra.actions.CassandraSelect;
import org.hcjf.names.Naming;
import org.hcjf.properties.*;
import org.hcjf.utils.Introspection;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraStorageSession extends StorageSession {

    private static final String SELECT_STATEMENT = "SELECT * FROM %s WHERE %s LIMIT %s;";

    private final Session session;
    private final CassandraStorageLayer layer;

    public CassandraStorageSession(String implName, Session session, CassandraStorageLayer layer) {
        super(implName);
        this.session = session;
        this.layer = layer;
    }

    public <R extends org.hcjf.layers.storage.actions.ResultSet> R executeQuery(
            Query query, String cqlStatement, List<Object> values, Class resultType) throws StorageAccessException {
        PreparedStatement statement = session.prepare(cqlStatement);
        com.datastax.driver.core.ResultSet cassandraResultSet =
                    session.execute(statement.bind(values.toArray()));

        Set<Row> rows = query.evaluate(cassandraResultSet.all(), new Query.Consumer<Row>() {

            @Override
            public <R> R get(Row row, String fieldName) {
                return (R) row.getObject(normalizeName(fieldName));
            }

        });

        org.hcjf.layers.storage.actions.ResultSet result;
        if(resultType != null) {
            List<Object> instances = new ArrayList<>();
            Map<String, Introspection.Setter> setters = Introspection.getSetters(resultType,layer.getNamingImplName());
            for (Row row : rows) {
                instances.add(createInstance(resultType, row, setters));
            }

            result = new CollectionResultSet(instances);
        } else {
            List<Map<String, Object>> resultRows = new ArrayList<>();
            Map<String, Object> map;
            for(Row row : cassandraResultSet) {
                map = new HashMap<>();
                for(ColumnDefinitions.Definition definition : row.getColumnDefinitions()) {
                    map.put(definition.getName(), row.getObject(definition.getName()));
                }
                resultRows.add(map);
            }

            result = new MapResultSet(resultRows);
        }

        try {
            return (R) result;
        } catch (ClassCastException ex) {
            throw new StorageAccessException("", ex);
        }
    }

    /**
     *
     * @param cqlStatement
     * @param values
     * @param resultType
     * @param <R>
     * @return
     * @throws StorageAccessException
     */
    public <R extends org.hcjf.layers.storage.actions.ResultSet> R execute(
            String cqlStatement, List<Object> values, Class resultType) throws StorageAccessException {
        PreparedStatement statement = session.prepare(cqlStatement);
        com.datastax.driver.core.ResultSet cassandraResultSet =
                session.execute(statement.bind(values.toArray()));

        org.hcjf.layers.storage.actions.ResultSet result;
        if(resultType != null) {
            List<Object> instances = new ArrayList<>();
            Map<String, Introspection.Setter> setters = Introspection.getSetters(resultType,layer.getNamingImplName());
            for (Row row : cassandraResultSet) {
                instances.add(createInstance(resultType, row, setters));
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
                    map.put(definition.getName(), row.getObject(definition.getName()));
                }
                rows.add(map);
            }

            result = new MapResultSet(rows);
        }

        try {
            return (R) result;
        } catch (ClassCastException ex) {
            throw new StorageAccessException("", ex);
        }
    }

    /**
     *
     * @param resultType
     * @param row
     * @param setters
     * @return
     * @throws StorageAccessException
     */
    private Object createInstance(Class resultType, Row row, Map<String, Introspection.Setter> setters) throws StorageAccessException {
        Object instance;
        Object rowValue;
        Introspection.Setter setter;

        try {
            instance = resultType.newInstance();
        } catch (Exception e) {
            throw new StorageAccessException("");
        }

        setters = Introspection.getSetters(resultType,layer.getNamingImplName());
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
                    throw new StorageAccessException("", ex);
                }
            }
        }
        return instance;
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
        session.close();
    }

    /**
     *
     * @param storageValueName
     * @return
     */
    public boolean checkColumn(String resourceName, String storageValueName) {
        boolean result = false;
        KeyspaceMetadata keyspaceMetadata =
                session.getCluster().getMetadata().getKeyspace(layer.getKeySpace());
        TableMetadata tableMetadata = keyspaceMetadata.getTable(resourceName);
        for(ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if(columnMetadata.getName().equals(storageValueName)) {
                result = true;
                break;
            }
        }
        return result;
    }

    /**
     *
     * @param name
     * @return
     */
    public final String normalizeName(String name) {
        return Naming.normalize(layer.getNamingImplName(), name);
    }

    /**
     *
     * @param resourceName
     * @return
     */
    public final List<String> getPartitionKey(String resourceName) {
        List<String> result = new ArrayList<>();
        TableMetadata metadata = session.getCluster().getMetadata().
                getKeyspace(layer.getKeySpace()).getTable(resourceName);
        result.addAll(metadata.getPartitionKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
        return result;
    }

    /**
     *
     * @param resourceName
     * @return
     */
    public final List<String> getClusteringKey(String resourceName) {
        List<String> result = new ArrayList<>();
        TableMetadata metadata = session.getCluster().getMetadata().
                getKeyspace(layer.getKeySpace()).getTable(resourceName);
        result.addAll(metadata.getClusteringColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
        return result;
    }

    /**
     *
     * @return
     * @throws StorageAccessException
     */
    @Override
    public Insert insert() throws StorageAccessException {
        return new CassandraInsert(this);
    }

    @Override
    public Select select(Query query) throws StorageAccessException {
        CassandraSelect result = new CassandraSelect(this);
        result.setQuery(query);
        return result;
    }
}
