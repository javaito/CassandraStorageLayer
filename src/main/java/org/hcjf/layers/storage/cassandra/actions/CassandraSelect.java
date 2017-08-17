package org.hcjf.layers.storage.cassandra.actions;

import org.hcjf.layers.query.*;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;
import org.hcjf.properties.SystemProperties;
import org.hcjf.utils.Strings;

import java.util.*;

/**
 * This class implements the select operation for the cassandra storage layer.
 * @author javaito
 */
public class CassandraSelect<C extends CassandraStorageSession> extends Select<C> {

    protected static final String SELECT_STATEMENT = "SELECT * FROM %s ";
    protected static final String SELECT_WHERE_STATEMENT = "WHERE %s ";
    protected static final String SELECT_LIMIT_STATEMENT = "LIMIT %s";
    protected static final String CONTAINS_RESERVED_WORD = "CONTAINS";

    public CassandraSelect(C session) {
        super(session);
    }

    @Override
    protected void onAdd(Object object) {
        setResultType(object.getClass());
        setResourceName(getSession().normalizeName(object.getClass().getSimpleName().toLowerCase()));
    }

    /**
     * Creates the query execution for the cassandra engine implementation.
     * @param params Query parameters.
     * @param <R> Expected result set.
     * @return Result set.
     * @throws StorageAccessException StorageAccessException
     */
    @Override
    public <R extends ResultSet> R execute(Object... params) throws StorageAccessException {
        Query query = getQuery();

        String resourceName = query.getResourceName();
        String normalizedResourceName = getSession().normalizeName(resourceName);

        //Creates the list of keys for the resource table.
        List<String> keys = getSession().getPartitionKey(normalizedResourceName);
        keys.addAll(getSession().getClusteringKey(normalizedResourceName));
        keys.addAll(getSession().getIndexes(normalizedResourceName));

        Map<String, Evaluator> evaluatorsByName = new LinkedHashMap<>();
        Map<String, List<Object>> valuesByName = new LinkedHashMap<>();
        Set<Evaluator> evaluators = query.getEvaluators();
        exploreQuery(evaluatorsByName, valuesByName, evaluators, keys, params);

        List<Object> values = new ArrayList<>();
        StringBuilder cqlStatement = new StringBuilder();
        if(query.getReturnParameters() == null || query.getReturnParameters().isEmpty()) {
            cqlStatement.append(String.format(SELECT_STATEMENT, normalizedResourceName));
        } else {
            cqlStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SELECT)).append(Strings.WHITE_SPACE);
            String argumentSeparatorValue = SystemProperties.get(SystemProperties.Query.ReservedWord.ARGUMENT_SEPARATOR);
            String argumentSeparator = Strings.EMPTY_STRING;
            Query.QueryReturnField queryReturnField;
            if(!query.returnAll()) {
                for (Query.QueryReturnParameter queryField : query.getReturnParameters()) {
                    if (queryField instanceof Query.QueryReturnField) {
                        queryReturnField = (Query.QueryReturnField) queryField;
                        cqlStatement.append(argumentSeparator);
                        cqlStatement.append(getSession().normalizeName(queryReturnField.getFieldName()));
                        if (queryReturnField.getAlias() != null && !queryReturnField.getAlias().isEmpty()) {
                            cqlStatement.append(Strings.WHITE_SPACE);
                            cqlStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.AS));
                            cqlStatement.append(Strings.WHITE_SPACE);
                            cqlStatement.append(getSession().normalizeName(queryReturnField.getAlias()));
                        }
                        cqlStatement.append(Strings.WHITE_SPACE);
                        argumentSeparator = argumentSeparatorValue;
                    }
                }
            } else {
                cqlStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.RETURN_ALL));
                cqlStatement.append(Strings.WHITE_SPACE);
            }
            cqlStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.FROM)).append(Strings.WHITE_SPACE);
            cqlStatement.append(normalizedResourceName).append(Strings.WHITE_SPACE);
        }

        Strings.Builder cqlWhereStatement = new Strings.Builder();
        for(String fieldName : evaluatorsByName.keySet()) {

            Evaluator evaluator = evaluatorsByName.get(fieldName);
            Query.skipEvaluator(evaluator);
            Class<Evaluator> evaluatorClass = (Class<Evaluator>) evaluator.getClass();

            if(Equals.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.EQUALS));
                cqlWhereStatement.append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE),
                        Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
                values.add(valuesByName.get(fieldName).get(0));
            } else if(GreaterThan.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.GREATER_THAN));
                cqlWhereStatement.append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE),
                        Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
                values.add(valuesByName.get(fieldName).get(0));
            } else if(GreaterThanOrEqual.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.GREATER_THAN_OR_EQUALS));
                cqlWhereStatement.append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE),
                        Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
                values.add(valuesByName.get(fieldName).get(0));
            } else if(SmallerThan.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SMALLER_THAN));
                cqlWhereStatement.append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE),
                        Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
                values.add(valuesByName.get(fieldName).get(0));
            } else if(SmallerThanOrEqual.class.equals(evaluatorClass)) {
                cqlWhereStatement.append(fieldName).append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.SMALLER_THAN_OR_EQUALS));
                cqlWhereStatement.append(Strings.WHITE_SPACE);
                cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE),
                        Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
                values.add(valuesByName.get(fieldName).get(0));
            } else if(In.class.equals(evaluatorClass)) {
                if(getSession().getColumnDataType(normalizedResourceName, fieldName).isCollection()) {
                    cqlWhereStatement.append(fieldName).append(Strings.WHITE_SPACE);
                    cqlWhereStatement.append(CONTAINS_RESERVED_WORD);
                    cqlWhereStatement.append(Strings.WHITE_SPACE);
                    cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE),
                            Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
                    values.add(valuesByName.get(fieldName).get(0));
                } else {
                    cqlWhereStatement.append(fieldName).append(Strings.WHITE_SPACE);
                    cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.IN));
                    cqlWhereStatement.append(Strings.START_GROUP);
                    Object iterableObject = valuesByName.get(fieldName).get(0);
                    Object[] collection;
                    if(iterableObject instanceof Collection) {
                        collection = ((Collection)iterableObject).toArray();
                    } else {
                        collection = (Object[]) iterableObject;
                    }
                    for (Object object : collection) {
                        cqlWhereStatement.append(SystemProperties.get(SystemProperties.Query.ReservedWord.REPLACEABLE_VALUE),
                                Strings.WHITE_SPACE, Strings.ARGUMENT_SEPARATOR);
                        values.add(object);
                    }
                    cqlWhereStatement.cleanBuffer();
                    cqlWhereStatement.append(Strings.WHITE_SPACE).append(Strings.END_GROUP,
                            Strings.WHITE_SPACE, SystemProperties.get(SystemProperties.Query.ReservedWord.AND), Strings.WHITE_SPACE);
                }
            }
        }

        if(cqlWhereStatement.length() > 0) {
            cqlStatement.append(String.format(SELECT_WHERE_STATEMENT, cqlWhereStatement.toString()));
        }

        Query reducedCopy = query.reduce(evaluatorsByName.values());

        if(reducedCopy.getEvaluators().isEmpty()) {
            if (query.getLimit() != null) {
                if (query.getStart() != null) {
                    cqlStatement.append(String.format(SELECT_LIMIT_STATEMENT, Integer.toString(query.getStart() + query.getLimit())));
                } else {
                    cqlStatement.append(String.format(SELECT_LIMIT_STATEMENT, query.getLimit().toString()));
                }
            }
        }

        return getSession().executeQuery(reducedCopy, cqlStatement.toString(), values, getResultType());
    }

    /**
     * Verify if the evaluator's field is into the list of keys in order to known which of the evaluators
     * is a candidate to create the cassandra statement.
     * @param evaluatorsByName Evaluators indexed by name.
     * @param valuesByName Values indexed by name.
     * @param evaluators All the evaluator of the related query.
     * @param keys Data base table keys.
     * @param params Query parameters.
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
