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
package com.github.paohaijiao.aggregate;


import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.convert.JQuickFlinkDataSetConverter;
import com.github.paohaijiao.function.FlinkAggregator;
import com.github.paohaijiao.function.JQuickFlinkAggregateFunction;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Flink聚合函数实现
 * 支持Map<String, List<Object>>格式的聚合配置
 */
public class JQuickDataSetFlinkAggregateFunction implements JQuickFlinkAggregateFunction<JQuickDataSet, JQuickDataSet> {

    private List<String> groupByColumns;

    private Map<String, List<Object>> aggregations;

    private String resultName = "aggregated_result";

    private String executionMode = "batch"; // batch, streaming, table

    public JQuickDataSetFlinkAggregateFunction() {
    }

    public JQuickDataSetFlinkAggregateFunction withGroupBy(List<String> groupByColumns) {
        this.groupByColumns = groupByColumns;
        return this;
    }

    public JQuickDataSetFlinkAggregateFunction withAggregations(Map<String, List<Object>> aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    public JQuickDataSetFlinkAggregateFunction withResultName(String resultName) {
        this.resultName = resultName;
        return this;
    }

    public JQuickDataSetFlinkAggregateFunction withExecutionMode(String mode) {
        this.executionMode = mode;
        return this;
    }

    @Override
    public JQuickDataSet aggregate(StreamExecutionEnvironment env,
                                   JQuickDataSet input,
                                   JQuickFunctionContext context) {
        if (input == null || input.isEmpty()) {
            return createEmptyResult(input);
        }

        try {
            if ("table".equals(executionMode)) {
                // 使用Table API
                return aggregateWithTableAPI(env, input);
            } else {
                // 使用DataStream API
                return aggregateWithDataStream(env, input);
            }
        } catch (Exception e) {
            throw new RuntimeException("Flink aggregation failed", e);
        }
    }

    /**
     * 使用DataStream API进行聚合
     */
    private JQuickDataSet aggregateWithDataStream(StreamExecutionEnvironment env, JQuickDataSet input) throws Exception {
        // 转换为DataStream
        DataStream<Row> dataStream = JQuickFlinkDataSetConverter.toDataStream(input, env);
        // 转换为Tuple2格式（分组键，行数据）
        DataStream<Tuple2<String, Row>> keyedStream;
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            keyedStream = dataStream.map(new KeyExtractor(groupByColumns));
        } else {
            // 无分组，使用常量键
            keyedStream = dataStream.map(row -> Tuple2.of("all", row))
                    .returns(Types.TUPLE(Types.STRING, Types.ROW_NAMED(getFieldNames(input), getFieldTypes(input))));
        }
        DataStream<Row> resultStream = performAggregation(keyedStream, input);// 执行聚合
        // 收集结果
        List<Row> results = new ArrayList<>();
        resultStream.executeAndCollect().forEachRemaining(results::add);
        return convertToDataSet(results, input);// 转换为JQuickDataSet
    }

    /**
     * 使用Table API进行聚合
     */
    private JQuickDataSet aggregateWithTableAPI(StreamExecutionEnvironment env, JQuickDataSet input) throws Exception {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 转换为Table
        String tempTableName = "temp_input_" + System.currentTimeMillis();
        Table inputTable = JQuickFlinkDataSetConverter.toTable(input, tableEnv, tempTableName);
        tableEnv.createTemporaryView(tempTableName, inputTable);
        // 构建SQL
        String sql = buildAggregationSQL(tempTableName);
        // 执行查询
        Table resultTable = tableEnv.sqlQuery(sql);
        // 收集结果
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
        List<Row> results = new ArrayList<>();
        resultStream.executeAndCollect().forEachRemaining(results::add);
        // 清理临时表
        tableEnv.dropTemporaryView(tempTableName);
        return convertToDataSet(results, input);
    }

    /**
     * 批处理模式（使用DataSet API）
     */
    public JQuickDataSet batchAggregate(JQuickDataSet input, JQuickFunctionContext context) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 转换为DataSet
        DataSet<Row> dataSet = JQuickFlinkDataSetConverter.toDataSet(input, env);

        // 转换为分组格式
        DataSet<Tuple2<String, Row>> groupedSet;
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            groupedSet = dataSet.map(new KeyExtractor(groupByColumns))
                    .returns(Types.TUPLE(Types.STRING, Types.ROW_NAMED(
                            getFieldNames(input), getFieldTypes(input))));
        } else {
            groupedSet = dataSet.map(row -> Tuple2.of("all", row))
                    .returns(Types.TUPLE(Types.STRING, Types.ROW_NAMED(
                            getFieldNames(input), getFieldTypes(input))));
        }

        // 执行聚合
        DataSet<Row> resultSet = performBatchAggregation(groupedSet, input);
        // 收集结果
        List<Row> results = resultSet.collect();
        return convertToDataSet(results, input);
    }

    /**
     * 执行聚合（分组）
     */
    private DataStream<Row> performAggregation(DataStream<Tuple2<String, Row>> keyedStream, JQuickDataSet input) {
        // 使用keyBy进行分组
        return keyedStream
                .keyBy(tuple -> tuple.f0)
                .aggregate(new FlinkAggregator(aggregations, groupByColumns));
    }

    /**
     * 批处理聚合
     */
    private DataSet<Row> performBatchAggregation(DataSet<Tuple2<String, Row>> groupedSet, JQuickDataSet input) {
        return groupedSet
                .groupBy(0)
                .reduceGroup(new GroupReduceAggregator(aggregations, groupByColumns));
    }

    /**
     * 构建聚合SQL
     */
    private String buildAggregationSQL(String tableName) {
        StringBuilder sql = new StringBuilder("SELECT ");
        // 添加分组列
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            sql.append(String.join(", ", groupByColumns));
        }

        // 添加聚合函数
        for (Map.Entry<String, List<Object>> entry : aggregations.entrySet()) {
            String functionName = entry.getKey();
            List<Object> columns = entry.getValue();
            for (Object col : columns) {
                String columnName = col.toString();
                if (groupByColumns != null && !groupByColumns.isEmpty()) {
                    sql.append(", ");
                } else if (sql.indexOf("SELECT") == sql.length() - "SELECT".length()) {
                    // 第一个聚合函数
                } else {
                    sql.append(", ");
                }
                String alias = functionName + "_" + columnName;
                sql.append(toFlinkSQLFunction(functionName, columnName)).append(" AS ").append(alias);
            }
        }

        sql.append(" FROM ").append(tableName);

        // 添加GROUP BY
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }

        return sql.toString();
    }

    /**
     * 转换为Flink SQL函数
     */
    private String toFlinkSQLFunction(String functionName, String columnName) {
        switch (functionName.toLowerCase()) {
            case "sum":
                return "SUM(" + columnName + ")";
            case "avg":
                return "AVG(" + columnName + ")";
            case "count":
                return "COUNT(" + columnName + ")";
            case "count_distinct":
                return "COUNT(DISTINCT " + columnName + ")";
            case "max":
                return "MAX(" + columnName + ")";
            case "min":
                return "MIN(" + columnName + ")";
            case "stddev":
                return "STDDEV(" + columnName + ")";
            case "variance":
                return "VARIANCE(" + columnName + ")";
            default:
                return functionName + "(" + columnName + ")";
        }
    }

    /**
     * 转换结果集
     */
    private JQuickDataSet convertToDataSet(List<Row> rows, JQuickDataSet original) {
        if (rows.isEmpty()) {
            return createEmptyResult(original);
        }

        // 构建列元数据
        List<JQuickColumnMeta> columns = new ArrayList<>();

        // 添加分组列
        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            for (String col : groupByColumns) {
                columns.add(new JQuickColumnMeta(col, String.class, "group_by"));
            }
        }

        // 添加聚合列
        for (Map.Entry<String, List<Object>> entry : aggregations.entrySet()) {
            String functionName = entry.getKey();
            for (Object col : entry.getValue()) {
                String columnName = col.toString();
                String alias = functionName + "_" + columnName;
                columns.add(new JQuickColumnMeta(alias, getResultType(functionName), "aggregation"));
            }
        }

        // 转换行数据
        List<JQuickRow> jquickRows = rows.stream()
                .map(row -> {
                    JQuickRow jrow = new JQuickRow();
                    for (int i = 0; i < columns.size() && i < row.getArity(); i++) {
                        jrow.put(columns.get(i).getName(), row.getField(i));
                    }
                    return jrow;
                })
                .collect(Collectors.toList());

        return new JQuickDataSet(columns, jquickRows);
    }

    /**
     * 创建空结果
     */
    private JQuickDataSet createEmptyResult(JQuickDataSet input) {
        List<JQuickColumnMeta> columns = new ArrayList<>();

        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            for (String col : groupByColumns) {
                columns.add(new JQuickColumnMeta(col, String.class, "group_by"));
            }
        }

        if (aggregations != null) {
            for (Map.Entry<String, List<Object>> entry : aggregations.entrySet()) {
                for (Object col : entry.getValue()) {
                    String alias = entry.getKey() + "_" + col.toString();
                    columns.add(new JQuickColumnMeta(alias, getResultType(entry.getKey()), "aggregation"));
                }
            }
        }

        return new JQuickDataSet(columns, Collections.emptyList());
    }

    /**
     * 获取结果类型
     */
    private Class<?> getResultType(String functionName) {
        switch (functionName.toLowerCase()) {
            case "count":
            case "count_distinct":
                return Long.class;
            default:
                return Double.class;
        }
    }

    /**
     * 获取字段名
     */
    private String[] getFieldNames(JQuickDataSet dataset) {
        return dataset.getColumns().stream()
                .map(JQuickColumnMeta::getName)
                .toArray(String[]::new);
    }

    /**
     * 获取字段类型
     */
    private TypeInformation<?>[] getFieldTypes(JQuickDataSet dataset) {
        return dataset.getColumns().stream()
                .map(col -> toFlinkTypeInfo(col.getType()))
                .toArray(TypeInformation<?>[]::new);
    }

    private TypeInformation<?> toFlinkTypeInfo(Class<?> javaType) {
        if (javaType == String.class) return Types.STRING;
        if (javaType == Integer.class || javaType == int.class) return Types.INT;
        if (javaType == Long.class || javaType == long.class) return Types.LONG;
        if (javaType == Double.class || javaType == double.class) return Types.DOUBLE;
        if (javaType == Float.class || javaType == float.class) return Types.FLOAT;
        if (javaType == Boolean.class || javaType == boolean.class) return Types.BOOLEAN;
        return Types.STRING;
    }

    /**
     * 键提取器
     */
    private static class KeyExtractor implements MapFunction<Row, Tuple2<String, Row>> {
        private final List<String> groupByColumns;

        public KeyExtractor(List<String> groupByColumns) {
            this.groupByColumns = groupByColumns;
        }

        @Override
        public Tuple2<String, Row> map(Row row) throws Exception {
            String key = groupByColumns.stream()
                    .map(col -> {
                        Object value = row.getField(col);
                        return value != null ? value.toString() : "null";
                    })
                    .collect(Collectors.joining("|"));
            return Tuple2.of(key, row);
        }
    }
}
