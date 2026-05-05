/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
 */
package com.github.paohaijiao.provider;

import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.*;

/**
 * 抽象分组聚合器 - 按分组字段进行聚合，返回 DataSet
 *
 * @param <R> 聚合结果类型
 */
public abstract class JQuickGroupByAggregationProvider<R> implements JQuickAggregationProvider<JQuickRow, JQuickDataSet> {

    protected final List<String> groupByColumns;

    protected final String resultColumnName;

    public JQuickGroupByAggregationProvider(List<String> groupByColumns, String resultColumnName) {
        this.groupByColumns = groupByColumns;
        this.resultColumnName = resultColumnName;
    }

    @Override
    public List<String> getColumns() {
        return groupByColumns;
    }


    @Override
    public JQuickDataSet apply(List<JQuickRow> rows) {
        return aggregate(rows);
    }

    @Override
    public JQuickDataSet aggregate(List<JQuickRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return new JQuickDataSet(Collections.emptyList(), Collections.emptyList());
        }
        Map<GroupKey, List<JQuickRow>> grouped = groupByKeys(rows);
        List<JQuickRow> resultRows = new ArrayList<>();
        for (Map.Entry<GroupKey, List<JQuickRow>> entry : grouped.entrySet()) {
            JQuickRow resultRow = new JQuickRow();
            GroupKey key = entry.getKey();
            for (int i = 0; i < groupByColumns.size(); i++) {
                resultRow.put(groupByColumns.get(i), key.getValues().get(i));
            }
            List<JQuickRow> groupRows = entry.getValue();
            R aggregatedValue = aggregateGroup(groupRows);
            resultRow.put(resultColumnName, aggregatedValue);
            resultRows.add(resultRow);
        }

        // 构建列元数据
        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (String col : groupByColumns) {
            columns.add(new JQuickColumnMeta(col, String.class, "group_by"));
        }
        columns.add(new JQuickColumnMeta(resultColumnName, getResultType(), "aggregation"));
        return new JQuickDataSet(columns, resultRows);
    }

    /**
     * 对分组进行聚合（由子类实现）
     *
     * @param groupRows 同一个分组的所有行
     * @return 聚合结果
     */
    protected abstract R aggregateGroup(List<JQuickRow> groupRows);

    /**
     * 获取结果类型（用于列元数据）
     */
    protected abstract Class<?> getResultType();

    /**
     * 按分组字段分组
     */
    private Map<GroupKey, List<JQuickRow>> groupByKeys(List<JQuickRow> rows) {
        Map<GroupKey, List<JQuickRow>> result = new LinkedHashMap<>(); // 使用 LinkedHashMap 保持顺序
        for (JQuickRow row : rows) {
            List<Object> keyValues = new ArrayList<>();
            for (String col : groupByColumns) {
                keyValues.add(row.get(col));
            }
            GroupKey key = new GroupKey(keyValues);
            result.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        return result;
    }

    /**
     * 分组键
     */
    private static class GroupKey {

        private final List<Object> values;

        public GroupKey(List<Object> values) {
            this.values = values;
        }

        public List<Object> getValues() {
            return values;
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
            return Objects.hash(values);
        }
    }
}