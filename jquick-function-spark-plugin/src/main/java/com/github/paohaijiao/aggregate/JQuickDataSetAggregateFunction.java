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

/**
 * packageName com.github.paohaijiao.aggregate
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.convert.SparkDataSetConverter;
import com.github.paohaijiao.domain.JQuickAggregationDomain;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import org.apache.spark.sql.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 支持Map<String, List<Object>>格式的聚合函数
 */
public class JQuickDataSetAggregateFunction implements JQuickSparkAggregateFunction<JQuickDataSet, JQuickDataSet> {

    private List<String> groupByColumns;

    private Map<String, List<Object>> aggregations;

    private String resultName = "aggregated_result";

    private List<JQuickAggregationDomain> aggregationConfigs;


    public JQuickDataSetAggregateFunction() {
    }

    public JQuickDataSetAggregateFunction withGroupBy(List<String> groupByColumns) {
        this.groupByColumns = groupByColumns;
        return this;
    }

    /**
     * 设置聚合配置
     *
     * @param aggregations Map格式：{"functionName": ["col1", "col2", ...]}
     *                     例如：{"sum": ["salary", "bonus"], "avg": ["age"]}
     */
    public JQuickDataSetAggregateFunction withAggregations(Map<String, List<Object>> aggregations) {
        this.aggregations = aggregations;
        this.aggregationConfigs = parseAggregations(aggregations);
        return this;
    }

    public JQuickDataSetAggregateFunction withResultName(String resultName) {
        this.resultName = resultName;
        return this;
    }

    @Override
    public JQuickDataSet aggregate(SparkSession spark, JQuickDataSet input, JQuickFunctionContext context) {
        if (input == null || input.isEmpty()) {
            return createEmptyResult(input);
        }
        Dataset<Row> df = SparkDataSetConverter.toSparkDataFrame(input, spark);// 转换为Spark DataFrame
        Dataset<Row> resultDf = performAggregation(df);// 执行聚合
        JQuickDataSet result = SparkDataSetConverter.fromSparkDataFrame(resultDf, resultName);// 转换回JQuickDataSet
        return enhanceColumnMetadata(result);// 更新列元数据的来源信息
    }

    /**
     * 解析聚合配置
     */
    private List<JQuickAggregationDomain> parseAggregations(Map<String, List<Object>> aggregations) {
        List<JQuickAggregationDomain> configs = new ArrayList<>();
        for (Map.Entry<String, List<Object>> entry : aggregations.entrySet()) {
            String functionName = entry.getKey();
            List<Object> columnObjects = entry.getValue();
            // 将Object转换为String（支持直接传String或包含String的List）
            List<String> columns = columnObjects.stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
            configs.add(new JQuickAggregationDomain(functionName, columns));
        }

        return configs;
    }

    /**
     * 执行聚合操作
     */
    private Dataset<Row> performAggregation(Dataset<Row> df) {
        if (aggregationConfigs == null || aggregationConfigs.isEmpty()) {
            throw new IllegalArgumentException("Aggregations cannot be null or empty");
        }

        if (groupByColumns != null && !groupByColumns.isEmpty()) {
            return performGroupByAggregation(df);
        } else {
            return performFullAggregation(df);
        }
    }

    /**
     * 分组聚合
     */
    private Dataset<Row> performGroupByAggregation(Dataset<Row> df) {
        Column[] groupByCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);// 创建分组列
        List<Column> allAggColumns = new ArrayList<>();// 构建所有聚合列
        for (JQuickAggregationDomain config : aggregationConfigs) {
            List<Column> aggCols = buildAggregationColumns(config);
            allAggColumns.addAll(aggCols);
        }
        if (allAggColumns.size() == 1) {// 执行分组聚合
            return df.groupBy(groupByCols).agg(allAggColumns.get(0));
        } else {
            Column firstAgg = allAggColumns.get(0);
            Column[] restAggs = allAggColumns.subList(1, allAggColumns.size()).toArray(new Column[0]);
            return df.groupBy(groupByCols).agg(firstAgg, restAggs);
        }
    }

    /**
     * 全量聚合（无分组）
     */
    private Dataset<Row> performFullAggregation(Dataset<Row> df) {
        List<Column> allAggColumns = new ArrayList<>();
        for (JQuickAggregationDomain config : aggregationConfigs) {
            List<Column> aggCols = buildAggregationColumns(config);
            allAggColumns.addAll(aggCols);
        }
        if (allAggColumns.size() == 1) {
            return df.agg(allAggColumns.get(0));
        } else {
            Column firstAgg = allAggColumns.get(0);
            Column[] restAggs = allAggColumns.subList(1, allAggColumns.size()).toArray(new Column[0]);
            return df.agg(firstAgg, restAggs);
        }
    }

    /**
     * 为聚合配置构建Spark列
     */
    private List<Column> buildAggregationColumns(JQuickAggregationDomain config) {
        String functionName = config.getFunctionName();
        List<Column> columns = new ArrayList<>();
        for (String columnName : config.getColumns()) {
            Column aggCol = createAggregateColumn(functionName, columnName);
            String alias = config.getColumnAliases().getOrDefault(columnName, functionName + "_" + columnName);
            columns.add(aggCol.as(alias));
        }
        return columns;
    }

    /**
     * 创建聚合列
     */
    private Column createAggregateColumn(String functionName, String columnName) {
        switch (functionName.toLowerCase()) {
            case "sum":
                return functions.sum(columnName);
            case "avg":
            case "average":
                return functions.avg(columnName);
            case "count":
                return functions.count(columnName);
            case "count_distinct":
            case "countdistinct":
                return functions.countDistinct(columnName);
            case "max":
                return functions.max(columnName);
            case "min":
                return functions.min(columnName);
            case "first":
                return functions.first(columnName);
            case "last":
                return functions.last(columnName);
            case "stddev":
            case "stddev_pop":
                return functions.stddev_pop(columnName);
            case "stddev_samp":
                return functions.stddev_samp(columnName);
            case "variance":
            case "var_pop":
                return functions.var_pop(columnName);
            case "var_samp":
                return functions.var_samp(columnName);
            case "collect_list":
                return functions.collect_list(columnName);
            case "collect_set":
                return functions.collect_set(columnName);
            case "approx_count_distinct":
                return functions.approx_count_distinct(columnName);
            case "mean":
                return functions.mean(columnName);
            default:
                throw new UnsupportedOperationException("Unsupported aggregate function: " + functionName);
        }
    }

    /**
     * 创建空结果
     */
    private JQuickDataSet createEmptyResult(JQuickDataSet input) {
        List<JQuickColumnMeta> columns = new ArrayList<>();
        if (groupByColumns != null && !groupByColumns.isEmpty()) {// 添加分组列
            for (String col : groupByColumns) {
                columns.add(new JQuickColumnMeta(col, String.class, "group_by"));
            }
        }
        // 添加聚合列
        for (JQuickAggregationDomain config : aggregationConfigs) {
            for (String col : config.getColumns()) {
                String alias = config.getColumnAliases().getOrDefault(col, config.getFunctionName() + "_" + col);
                columns.add(new JQuickColumnMeta(alias, getResultType(config.getFunctionName()), "aggregation"));
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
            case "approx_count_distinct":
                return Long.class;
            case "collect_list":
                return List.class;
            case "collect_set":
                return Set.class;
            default:
                return Double.class;
        }
    }

    /**
     * 增强列元数据
     */
    private JQuickDataSet enhanceColumnMetadata(JQuickDataSet result) {
        // 这里可以对结果集的列元数据进行增强
        // 例如添加聚合函数的来源信息等
        return result;
    }

    @Override
    public String name() {
        return "JQuickDataSetAggregateFunctionV2";
    }
}