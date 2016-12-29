package org.hcjf.layers.storage.cassandra.actions;

import com.sun.javafx.binding.StringFormatter;
import org.hcjf.layers.query.*;
import org.hcjf.layers.storage.StorageAccessException;
import org.hcjf.layers.storage.actions.ResultSet;
import org.hcjf.layers.storage.actions.Select;
import org.hcjf.layers.storage.cassandra.CassandraStorageSession;
import org.hcjf.names.CassandraNaming;
import org.hcjf.names.Naming;

import java.util.*;

/**
 * @author javaito
 * @mail javaito@gmail.com
 */
public class CassandraSelect extends Select<CassandraStorageSession> {

    private static final String SELECT_STATEMENT = "SELECT * FROM %s ";
    private static final String SELECT_WHERE_STATEMENT = "WHERE %s ";
    private static final String SELECT_LIMIT_STATEMENT = "LIMIT %s";
    private static final String WHERE_SEPARATOR = " AND ";

    public CassandraSelect(CassandraStorageSession session) {
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
    public <R extends ResultSet> R execute() throws StorageAccessException {
        Query query = getQuery();

        List<String> keys = getSession().getPartitionKey(getSession().normalizeName(getResourceName()));
        keys.addAll(getSession().getClusteringKey(getSession().normalizeName(getResourceName())));

        Map<String, Evaluator> evaluatorsByName = new LinkedHashMap<>();
        Map<String, List<Object>> valuesByName = new LinkedHashMap<>();
        Set<Evaluator> evaluators = query.getEvaluators();
        boolean totalBreak = false;
        for(String partitionKey : keys) {
            Iterator<Evaluator> iterator = evaluators.iterator();
            while(iterator.hasNext()) {
                Evaluator evaluator = iterator.next();
                if(!(evaluator instanceof FieldEvaluator)) {
                    continue;
                }
                FieldEvaluator fieldEvaluator = (FieldEvaluator) evaluator;
                if(getSession().normalizeName(fieldEvaluator.getFieldName()).equals(partitionKey)) {
                    if(evaluatorsByName.containsKey(fieldEvaluator.getFieldName())) {
                        if(fieldEvaluator.getClass().equals(evaluatorsByName.get(fieldEvaluator.getFieldName()).getClass())) {
                            valuesByName.get(fieldEvaluator.getFieldName()).add(fieldEvaluator.getValue());
                            totalBreak = false;
                        } else {
                            evaluatorsByName.remove(fieldEvaluator.getFieldName());
                            valuesByName.remove(fieldEvaluator.getFieldName());
                            totalBreak = true;
                        }
                    } else {
                        evaluatorsByName.put(fieldEvaluator.getFieldName(), fieldEvaluator);
                        valuesByName.put(fieldEvaluator.getFieldName(), new ArrayList<>());
                        valuesByName.get(fieldEvaluator.getFieldName()).add(fieldEvaluator.getValue());
                        totalBreak = false;
                    }
                } else {
                    totalBreak = true;
                }
            }
            if(totalBreak) {
                break;
            }
        }

        List<Object> values = new ArrayList<>();
        StringBuilder cqlStatement = new StringBuilder();
        cqlStatement.append(String.format(SELECT_STATEMENT, getSession().normalizeName(getResourceName())));

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
                cqlWhereStatement.append(fieldName).append(" IN ( ?");
                values.add(valuesByName.get(fieldName).get(0));
                for (int i = 1; i < values.size(); i++) {
                    cqlWhereStatement.append(fieldName).append(", ?");
                    values.add(valuesByName.get(fieldName).get(i));
                }
                cqlWhereStatement.append(fieldName).append(" )");
            }
            separator = WHERE_SEPARATOR;
        }

        if(cqlWhereStatement.length() > 0) {
            cqlStatement.append(String.format(SELECT_WHERE_STATEMENT, cqlWhereStatement.toString()));
        }
        cqlStatement.append(String.format(SELECT_LIMIT_STATEMENT, query.getLimit().toString()));

        Query reducedCopy = query.reduce(evaluatorsByName.values(), null);
        return getSession().executeQuery(reducedCopy, cqlStatement.toString(), values, getResultType());
    }
}
