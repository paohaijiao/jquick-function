package com.github.paohaijiao.transform.aggegate;

import com.github.paohaijiao.domain.JQuickAggregator;
import com.github.paohaijiao.group.JQuickGroupByKeyDomain;
import com.github.paohaijiao.provider.aggregate.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.transform.JQuickDataSetTransformer;

import java.util.*;

public class JQuickAggregateTransformer extends JQuickDataSetTransformer {

    private final List<String> groupByColumns;

    private final Map<JQuickGroupByKeyDomain, JQuickRow> aggregateResults = new HashMap<>();

    public JQuickAggregateTransformer(JQuickDataSet inputDataSet, List<String> groupByColumns, List<JQuickFunctionProvider<?, ?>> providers) {
        super(inputDataSet, providers);
        this.groupByColumns = groupByColumns;
    }

    @Override
    protected void transformRow(JQuickRow sourceRow, JQuickRow targetRow, List<JQuickFunctionProvider<?, ?>> providers, int rowIndex) {
        JQuickGroupByKeyDomain groupByKeyDomain = new JQuickGroupByKeyDomain(sourceRow, groupByColumns);
        JQuickRow aggRow = aggregateResults.computeIfAbsent(groupByKeyDomain, k -> {
            JQuickRow newRow = new JQuickRow();
            for (String col : groupByColumns) {
                newRow.put(col, sourceRow.get(col));
            }
            for (JQuickFunctionProvider<?, ?> provider : providers) {   //初始化聚合值
                if (provider instanceof JQuickAbstractAggregationProvider) {
                    JQuickAbstractAggregationProvider<?> aggProvider = (JQuickAbstractAggregationProvider<?>) provider;
                    newRow.put(provider.getTargetField(), aggProvider.getInitialValue());
                } else {
                    newRow.put(provider.getTargetField(), null);
                }
            }
            return newRow;
        });
        for (JQuickFunctionProvider<?, ?> provider : providers) {//对相关值进行迭代
            String targetField = provider.getTargetField();
            Object currentValue = aggRow.get(targetField);
            Object rowValue = ((JQuickFunctionProvider<JQuickRow, Object>) provider).apply(sourceRow);
            if (provider instanceof JQuickAbstractAggregationProvider) {
                @SuppressWarnings("unchecked")
                JQuickAbstractAggregationProvider<Object> aggProvider = (JQuickAbstractAggregationProvider<Object>) provider;
                Object newValue = aggProvider.accumulate(currentValue, rowValue);
                aggRow.put(targetField, newValue);
            } else {// 非聚合函数，直接覆盖（取最后一个值）
                aggRow.put(targetField, rowValue);
            }
        }
    }

    @Override
    protected void postProcess() {
        transformedRows.clear();
        for (Map.Entry<JQuickGroupByKeyDomain, JQuickRow> entry : aggregateResults.entrySet()) {
            JQuickRow row = entry.getValue();
            JQuickRow resultRow = new JQuickRow();
            for (String col : groupByColumns) {
                resultRow.put(col, row.get(col));
            }
            for (JQuickFunctionProvider<?, ?> provider : providers) {
                String targetField = provider.getTargetField();
                Object value = row.get(targetField);
                if (provider instanceof JQuickAbstractAggregationProvider) {
                    JQuickAggregator aggregator = (JQuickAggregator) value;
                    resultRow.put(targetField, aggregator.getResult());
                } else {
                    resultRow.put(targetField, value);
                }
            }
            transformedRows.add(resultRow);
        }
    }
    @Override
    protected JQuickDataSet buildDataSet() {
        // 构建列元数据
        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (String col : groupByColumns) {
            JQuickColumnMeta meta = inputDataSet.getColumns().stream()
                    .filter(c -> c.getName().equals(col))
                    .findFirst()
                    .orElse(new JQuickColumnMeta(col, Object.class, "group_by"));
            columns.add(meta);
        }
        for (JQuickFunctionProvider<?, ?> provider : providers) {
            columns.add(new JQuickColumnMeta(provider.getTargetField(), provider.getTargetClass(), "aggregate"));
        }
        return new JQuickDataSet(columns, transformedRows);
    }

}