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
import com.github.paohaijiao.convert.SparkDataSetConverter;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * Spark分布式分组聚合器抽象基类
 * 利用Spark的分布式计算能力进行高效聚合
 *
 * @param <R> 聚合结果类型
 */
public abstract class JQuickSparkGroupByAggregationProvider<R> implements JQuickAggregationProvider<JQuickRow, JQuickDataSet> {

    protected final List<String> groupByColumns;

    protected final String resultColumnName;

    protected final SparkSession spark;

    public JQuickSparkGroupByAggregationProvider(List<String> groupByColumns, String resultColumnName, SparkSession spark) {
        this.groupByColumns = groupByColumns;
        this.resultColumnName = resultColumnName;
        this.spark = spark;
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
     * 使用Spark进行分布式聚合
     */
    @Override
    public JQuickDataSet aggregate(List<JQuickRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return new JQuickDataSet(Collections.emptyList(), Collections.emptyList());
        }
        JQuickDataSet tempDataSet = buildTempDataSet(rows);// 转换为Spark DataFrame
        Dataset<Row> df = SparkDataSetConverter.toSparkDataFrame(tempDataSet, spark);
        Dataset<Row> aggregatedDf = doAggregate(df);
        return SparkDataSetConverter.fromSparkDataFrame(aggregatedDf, "aggregated_result");
    }

    /**
     * 执行Spark聚合（子类实现）
     */
    protected abstract Dataset<Row> doAggregate(Dataset<Row> df);


    /**
     * 构建临时数据集（用于转换）
     */
    private JQuickDataSet buildTempDataSet(List<JQuickRow> rows) {
        List<JQuickColumnMeta> columns = new ArrayList<>();
        if (!rows.isEmpty()) {
            JQuickRow firstRow = rows.get(0);
            for (String col : groupByColumns) {
                Object value = firstRow.get(col);
                Class<?> type = value != null ? value.getClass() : String.class;
                columns.add(new JQuickColumnMeta(col, type, "source"));
            }
            addAggregateColumns(columns, firstRow);
        }
        return new JQuickDataSet(columns, rows);
    }

    /**
     * 添加聚合列（子类可重写）
     */
    protected void addAggregateColumns(List<JQuickColumnMeta> columns, JQuickRow sampleRow) {

    }

    /**
     * 创建结果列元数据
     */
    protected List<JQuickColumnMeta> buildResultColumns() {
        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (String col : groupByColumns) {
            columns.add(new JQuickColumnMeta(col, String.class, "group_by"));
        }
        columns.add(new JQuickColumnMeta(resultColumnName, Object.class, "aggregation"));
        return columns;
    }
}