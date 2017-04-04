package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.query.*;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;

import java.util.*;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraSelect<C extends CassandraStorageSession> extends Select<C> {

    private static final String SELECT_STATEMENT = "SELECT * FROM %s ";
    private static final String SELECT_WHERE_STATEMENT = "WHERE %s ";
    private static final String SELECT_LIMIT_STATEMENT = "LIMIT %s";
    private static final String WHERE_SEPARATOR = " AND ";

    public CassandraSelect(C session) {
        super(session);
    }

    @Override
    protected void onAdd(Object object) {
        setResultType(object.getClass());
        setResourceName(getSession().normalizeName(object.getClass().getSimpleName().toLowerCase()));
    }

    /**
     * @return
     */
    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        Query query = getQuery();

        String resourceName = query.getResourceName();
        String normalizedResourceName = getSession().normalizeName(resourceName);

        List<String> keys = getSession().getPartitionKey(normalizedResourceName);
        keys.addAll(getSession().getClusteringKey(normalizedResourceName));
        keys.addAll(getSession().getIndexes(normalizedResourceName));

        Map<String, Evaluator> evaluatorsByName = new LinkedHashMap<>();
        Map<String, List<Object>> valuesByName = new LinkedHashMap<>();
        Set<Evaluator> evaluators = query.getEvaluators();
        exploreQuery(evaluatorsByName, valuesByName, evaluators, keys, params);

        List<Object> values = new ArrayList<>();
        StringBuilder cqlStatement = new StringBuilder();
        cqlStatement.append(String.format(SELECT_STATEMENT, normalizedResourceName));

        StringBuilder cqlWhereStatement = new StringBuilder();
        String separator = "";
        for(String fieldName : evaluatorsByName.keySet()) {
            cqlWhereStatement.append(separator);
            Class<Evaluator> evaluatorClass = (Class<Evaluator>) evaluatorsByName.get(fieldName).getClass();
            if(Equals.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(" = ?");
                values.add(valuesByName.get(fieldName).get(0));
            } else if(GreaterThan.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(" > ?");
                values.add(valuesByName.get(fieldName).get(0));
            } else if(GreaterThanOrEqual.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(" >= ?");
                values.add(valuesByName.get(fieldName).get(0));
            } else if(SmallerThan.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(" < ?");
                values.add(valuesByName.get(fieldName).get(0));
            } else if(SmallerThanOrEqual.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(" <= ?");
                values.add(valuesByName.get(fieldName).get(0));
            } else if(In.class.equals(evaluatorClass)) {
                if(getSession().getColumnDataType(normalizedResourceName, fieldName).isCollection()) {
                    cqlWhereStatement.append(fieldName).append(" CONTAINS ?");
                    values.add(valuesByName.get(fieldName).get(0));
                } else {
                    cqlWhereStatement.append(fieldName).append(" IN (");
                    Object[] collection = (Object[]) valuesByName.get(fieldName).get(0);
                    String inSeparator = "";
                    for (Object object : collection) {
                        cqlWhereStatement.append(inSeparator);
                        cqlWhereStatement.append("?");
                        values.add(object);
                        inSeparator = " ,";
                    }
                    cqlWhereStatement.append(" )");
                }
            }
            separator = WHERE_SEPARATOR;
        }

        if(cqlWhereStatement.length() > 0) {
            cqlStatement.append(String.format(SELECT_WHERE_STATEMENT, cqlWhereStatement.toString()));
        }

        if(query.getLimit() != null) {
            cqlStatement.append(String.format(SELECT_LIMIT_STATEMENT, query.getLimit().toString()));
        }

        Query reducedCopy = query.reduce(evaluatorsByName.values());
        return getSession().executeQuery(reducedCopy, cqlStatement.toString(), values, getResultType());
    }

    /**
     *
     * @param evaluatorsByName
     * @param valuesByName
     * @param evaluators
     * @param keys
     * @param params
     */
    private void exploreQuery(Map<String, Evaluator> evaluatorsByName,
                              Map<String, List<Object>> valuesByName,
                              Set<Evaluator> evaluators, List<String> keys, Object... params) {
        Iterator<Evaluator> iterator = evaluators.iterator();
        while(iterator.hasNext()) {
            Evaluator evaluator = iterator.next();
            if(evaluator instanceof EvaluatorCollection) {
                exploreQuery(evaluatorsByName, valuesByName, ((EvaluatorCollection)evaluator).getEvaluators(), keys, params);
            } else {
                FieldEvaluator fieldEvaluator = (FieldEvaluator) evaluator;
                String normalizedFieldName = getSession().normalizeName(((Query.QueryField)fieldEvaluator.getQueryParameter()).getFieldName());
                if (keys.contains(normalizedFieldName)) {
                    if (evaluatorsByName.containsKey(normalizedFieldName)) {
                        if (fieldEvaluator.getClass().equals(evaluatorsByName.get(normalizedFieldName).getClass())) {
                            valuesByName.get(normalizedFieldName).add(fieldEvaluator.getValue(null,null, null, params));
                        } else {
                            evaluatorsByName.remove(normalizedFieldName);
                            valuesByName.remove(normalizedFieldName);
                        }
                    } else {
                        evaluatorsByName.put(normalizedFieldName, fieldEvaluator);
                        valuesByName.put(normalizedFieldName, new ArrayList<>());
                        valuesByName.get(normalizedFieldName).add(fieldEvaluator.getValue(null,null, null, params));
                    }
                }
            }
        }
    }
}
