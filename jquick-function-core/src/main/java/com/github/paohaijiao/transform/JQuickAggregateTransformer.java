package com.github.paohaijiao.transform;

import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.*;
import java.util.stream.Collectors;

public class JQuickAggregateTransformer extends JQuickDataSetTransformer {

    private final List<String> groupByColumns;

    private final Map<GroupKey, JQuickRow> aggregateResults = new HashMap<>();

    public JQuickAggregateTransformer(JQuickDataSet inputDataSet, List<String> groupByColumns, List<JQuickFunctionProvider<?, ?>> providers) {
        super(inputDataSet, providers);
        this.groupByColumns = groupByColumns;
    }

    @Override
    protected void transformRow(JQuickRow sourceRow, JQuickRow targetRow, List<JQuickFunctionProvider<?, ?>> providers, int rowIndex) {
        GroupKey key = new GroupKey(sourceRow, groupByColumns);
        // 获取或创建聚合行
        JQuickRow aggRow = aggregateResults.computeIfAbsent(key, k -> {
            JQuickRow newRow = new JQuickRow();
            // 设置分组字段值
            for (String col : groupByColumns) {
                newRow.put(col, sourceRow.get(col));
            }
            // 初始化各聚合字段的初始值
            for (JQuickFunctionProvider<?, ?> provider : providers) {
                if (provider instanceof JQuickAbstractAggregationProvider) {
                    JQuickAbstractAggregationProvider<?> aggProvider = (JQuickAbstractAggregationProvider<?>) provider;
                    newRow.put(provider.getTargetField(), aggProvider.getInitialValue());
                } else {
                    newRow.put(provider.getTargetField(), null);
                }
            }
            return newRow;
        });
        // 处理每个字段映射
        for (JQuickFunctionProvider<?, ?> provider : providers) {
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
        transformedRows.addAll(aggregateResults.values());
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

    /**
     * 分组键
     */
    private static class GroupKey {
        private final List<Object> values;

        GroupKey(JQuickRow row, List<String> columns) {
            this.values = columns.stream().map(row::get).collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupKey groupKey = (GroupKey) o;
            return Objects.equals(values, groupKey.values);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(values);
        }
    }
}