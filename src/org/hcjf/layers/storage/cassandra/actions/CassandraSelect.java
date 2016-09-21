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

        Map<String, Class<? extends Evaluator>> evaluatorsByName = new LinkedHashMap<>();
        Map<String, List<Object>> valuesByName = new LinkedHashMap<>();
        Set<Evaluator> evaluators = query.getEvaluators();
        boolean totalBreak = false;
        for(String partitionKey : keys) {
            Iterator<Evaluator> iterator = evaluators.iterator();
            while(iterator.hasNext()) {
                Evaluator evaluator = iterator.next();
                if(getSession().normalizeName(evaluator.getFieldName()).equals(partitionKey)) {
                    if(evaluatorsByName.containsKey(evaluator.getFieldName())) {
                        if(evaluator.getClass().equals(evaluatorsByName.get(evaluator.getFieldName()))) {
                            valuesByName.get(evaluator.getFieldName()).add(evaluator.getValue());
                            totalBreak = false;
                        } else {
                            evaluatorsByName.remove(evaluator.getFieldName());
                            valuesByName.remove(evaluator.getFieldName());
                            totalBreak = true;
                        }
                    } else {
                        evaluatorsByName.put(evaluator.getFieldName(), evaluator.getClass());
                        valuesByName.put(evaluator.getFieldName(), new ArrayList<>());
                        valuesByName.get(evaluator.getFieldName()).add(evaluator.getValue());
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
            if(Equals.class.equals(evaluatorsByName.get(fieldName))) {
                cqlWhereStatement.append(fieldName).append(" = ?");
                values.add(valuesByName.get(fieldName).get(0));
            } else if(GreaterThan.class.equals(evaluatorsByName.get(fieldName))) {

            } else if(GreaterThanOrEqual.class.equals(evaluatorsByName.get(fieldName))) {

            } else if(SmallerThan.class.equals(evaluatorsByName.get(fieldName))) {

            } else if(SmallerThanOrEqual.class.equals(evaluatorsByName.get(fieldName))) {

            } else if(In.class.equals(evaluatorsByName.get(fieldName))) {

            }
            separator = WHERE_SEPARATOR;
        }

        if(cqlWhereStatement.length() > 0) {
            cqlStatement.append(String.format(SELECT_WHERE_STATEMENT, cqlWhereStatement.toString()));
        }
        cqlStatement.append(String.format(SELECT_LIMIT_STATEMENT, query.getLimit().toString()));

        ResultSet sessionResultSet = getSession().executeQuery(query, cqlStatement.toString(), values, getResultType());

        return null;
    }
}
