package com.github.paohaijiao.transform;


import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 支持聚合查询的 SELECT 转换器
 * 类似于 GROUP BY + 聚合函数
 */
public class JQuickAggregateTransformer extends JQuickDataSetTransformer {

    /** Group By 的字段名 */
    private final List<String> groupByColumns;

    /** 聚合结果缓存 */
    private final Map<GroupKey, JQuickRow> aggregateResults = new HashMap<>();

    public JQuickAggregateTransformer(JQuickDataSet inputDataSet, List<String> groupByColumns, List<JQuickFunctionProvider<?, ?>> providers) {
        super(inputDataSet, providers);
        this.groupByColumns = groupByColumns;
    }

    @Override
    protected void transformRow(JQuickRow sourceRow, JQuickRow targetRow, List<JQuickFunctionProvider<?, ?>> fieldMappings, int rowIndex) {
        GroupKey key = new GroupKey(sourceRow, groupByColumns);
        // 获取或创建聚合行
        JQuickRow aggRow = aggregateResults.computeIfAbsent(key, k -> {
            JQuickRow newRow = new JQuickRow();
            for (String col : groupByColumns) {
                newRow.put(col, sourceRow.get(col));
            }
            return newRow;
        });

        // 应用聚合函数（这里需要支持累加）
        for (JQuickFunctionProvider<?, ?> provider : fieldMappings) {
            String targetField = provider.getTargetField();
            Object currentValue = aggRow.get(targetField);
            Object newValue = ((JQuickFunctionProvider<JQuickRow, Object>) provider).apply(sourceRow);
            // 如果是聚合函数，需要累加
            if (provider instanceof SumProvider) {
                double sum = (currentValue instanceof Number ? ((Number) currentValue).doubleValue() : 0.0) + (newValue instanceof Number ? ((Number) newValue).doubleValue() : 0.0);
                aggRow.put(targetField, sum);
            } else if (provider instanceof CountProvider) {
                long count = (currentValue instanceof Number ? ((Number) currentValue).longValue() : 0L) + 1;
                aggRow.put(targetField, count);
            } else {
                // 非聚合函数，直接覆盖（取最后一个值）
                aggRow.put(targetField, newValue);
            }
        }
    }

    @Override
    protected void postProcess() {
        // 将聚合结果转换回行列表
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
            this.values = columns.stream()
                    .map(row::get)
                    .collect(Collectors.toList());
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
