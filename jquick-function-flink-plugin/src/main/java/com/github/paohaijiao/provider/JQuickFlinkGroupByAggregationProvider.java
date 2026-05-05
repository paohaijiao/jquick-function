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

import com.github.paohaijiao.convert.JQuickFlinkDataSetConverter;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class JQuickFlinkGroupByAggregationProvider<R> implements JQuickAggregationProvider<JQuickRow, JQuickDataSet> {

    protected final List<String> groupByColumns;

    protected final String resultColumnName;

    protected final ExecutionEnvironment env;

    protected final StreamTableEnvironment tableEnv;

    public JQuickFlinkGroupByAggregationProvider(List<String> groupByColumns, String resultColumnName, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        this.groupByColumns = groupByColumns;
        this.resultColumnName = resultColumnName;
        this.env = env;
        this.tableEnv = tableEnv;
    }

    @Override
    public List<String> getColumns() {
        return groupByColumns;
    }

    @Override
    public JQuickDataSet apply(List<JQuickRow> rows) {
        return aggregate(rows);
    }

    /**
     * 使用Flink进行分布式聚合
     */
    @Override
    public JQuickDataSet aggregate(List<JQuickRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return new JQuickDataSet(Collections.emptyList(), Collections.emptyList());
        }
        try {
            DataSet<JQuickRow> dataSet = JQuickFlinkDataSetConverter.toFlinkDataSet(rows, env);
            DataSet<JQuickRow> aggregatedDs = doAggregate(dataSet);
            List<JQuickRow> results = aggregatedDs.collect();
            return buildResultDataSet(results);
        } catch (Exception e) {
            throw new RuntimeException("Flink aggregation failed", e);
        }
    }

    /**
     * 执行Flink聚合（子类实现）
     */
    protected abstract DataSet<JQuickRow> doAggregate(DataSet<JQuickRow> dataSet) throws Exception;

    /**
     * 构建结果数据集
     */
    protected JQuickDataSet buildResultDataSet(List<JQuickRow> results) {
        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (String col : groupByColumns) {
            columns.add(new JQuickColumnMeta(col, Object.class, "group_by"));
        }
        columns.add(new JQuickColumnMeta(resultColumnName, getResultType(), "aggregation"));
        return new JQuickDataSet(columns, results);
    }

    /**
     * 获取结果类型
     */
    protected abstract Class<?> getResultType();

    /**
     * 创建分组键的辅助方法
     */
    protected String createGroupKey(JQuickRow row) {
        StringBuilder key = new StringBuilder();
        for (String col : groupByColumns) {
            Object value = row.get(col);
            key.append(value != null ? value.toString() : "null").append("|");
        }
        return key.toString();
    }

    /**
     * 从分组键解析出行数据的辅助方法
     */
    protected JQuickRow parseGroupKey(String groupKey, JQuickRow template) {
        JQuickRow result = new JQuickRow();
        String[] parts = groupKey.split("\\|");
        for (int i = 0; i < groupByColumns.size() && i < parts.length; i++) {
            String value = parts[i];
            if (!"null".equals(value)) {
                result.put(groupByColumns.get(i), value);
            }
        }
        return result;
    }
}