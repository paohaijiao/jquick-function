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
package com.github.paohaijiao.dataset;

import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.provider.impl.CountProvider;
import com.github.paohaijiao.provider.impl.SumProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Spark 分布式计算集成
 * 利用 JQuickFunctionProvider 的可序列化特性，在 Spark 集群上执行转换
 */
public class JQuickSparkEngine implements Serializable {

    private final SparkSession spark;
    private final JavaSparkContext sc;

    public JQuickSparkEngine(SparkSession spark) {
        this.spark = spark;
        this.sc = new JavaSparkContext(spark.sparkContext());
    }

    /**
     * 将 JQuickDataSet 转换为 Spark RDD
     */
    public JavaRDD<JQuickRow> toRDD(JQuickDataSet dataSet) {
        return sc.parallelize(dataSet.getRows());
    }

    /**
     * 在 Spark 上执行 SELECT 转换（分布式）
     *
     * @param dataSet 输入数据集
     * @param providers 转换 Provider（必须是 Serializable）
     * @return 转换后的 JQuickDataSet
     */
    public JQuickDataSet select(JQuickDataSet dataSet, List<JQuickFunctionProvider<?, ?>> providers) {
        JavaRDD<JQuickRow> inputRDD = toRDD(dataSet);
        // 广播 providers 到所有 Executor（因为它们是 Serializable）
        List<JQuickFunctionProvider<?, ?>> broadcastProviders = sc.broadcast(providers).value();
        // 分布式转换：每个 Executor 独立处理自己的分区
        JavaRDD<JQuickRow> transformedRDD = inputRDD.map(row -> {
            JQuickRow resultRow = new JQuickRow();
            for (JQuickFunctionProvider<?, ?> provider : broadcastProviders) {
                @SuppressWarnings("unchecked")
                JQuickFunctionProvider<JQuickRow, Object> typedProvider = (JQuickFunctionProvider<JQuickRow, Object>) provider;
                Object value = typedProvider.apply(row);
                resultRow.put(provider.getTargetField(), value);
            }
            return resultRow;
        });
        // 收集结果并构建列元数据
        List<JQuickColumnMeta> columns = providers.stream()
                .map(p -> new JQuickColumnMeta(p.getTargetField(), p.getTargetClass(), "spark_select"))
                .collect(Collectors.toList());
        return new JQuickDataSet(columns, transformedRDD.collect());
    }

    /**
     * 在 Spark 上执行 SELECT 转换（保持原始列）
     */
    public JQuickDataSet selectKeepOriginal(JQuickDataSet dataSet, List<JQuickFunctionProvider<?, ?>> providers, List<String> excludedColumns) {
        JavaRDD<JQuickRow> inputRDD = toRDD(dataSet);
        List<JQuickFunctionProvider<?, ?>> broadcastProviders = sc.broadcast(providers).value();
        Set<String> excludedSet = new HashSet<>(excludedColumns);

        // 获取原始列名
        List<String> originalColumns = dataSet.getColumnNames();
        JavaRDD<JQuickRow> transformedRDD = inputRDD.map(row -> {
            JQuickRow resultRow = new JQuickRow();
            // 保留原始列（排除指定的）
            for (String col : originalColumns) {
                if (!excludedSet.contains(col)) {
                    resultRow.put(col, row.get(col));
                }
            }
            // 添加 Provider 转换的列
            for (JQuickFunctionProvider<?, ?> provider : broadcastProviders) {
                @SuppressWarnings("unchecked")
                JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                        (JQuickFunctionProvider<JQuickRow, Object>) provider;
                Object value = typedProvider.apply(row);
                resultRow.put(provider.getTargetField(), value);
            }
            return resultRow;
        });

        // 构建列元数据
        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (String col : originalColumns) {
            if (!excludedSet.contains(col)) {
                columns.add(new JQuickColumnMeta(col, Object.class, "original"));
            }
        }
        for (JQuickFunctionProvider<?, ?> provider : providers) {
            columns.add(new JQuickColumnMeta(provider.getTargetField(), provider.getTargetClass(), "derived"));
        }

        return new JQuickDataSet(columns, transformedRDD.collect());
    }

    /**
     * 在 Spark 上执行 GROUP BY + 聚合（分布式 Shuffle）
     */
    public JQuickDataSet aggregate(JQuickDataSet dataSet, List<String> groupByColumns, List<JQuickFunctionProvider<?, ?>> aggProviders) {
        JavaRDD<JQuickRow> inputRDD = toRDD(dataSet);
        // 广播聚合函数
        List<JQuickFunctionProvider<?, ?>> broadcastAggProviders = sc.broadcast(aggProviders).value();

        // 第一阶段：在每个分区内进行局部聚合（Map-side Combine）
        JavaRDD<JQuickRow> partialAggRDD = inputRDD.mapPartitions(iterator -> {
            Map<GroupKey, JQuickRow> partialResults = new HashMap<>();
            while (iterator.hasNext()) {
                JQuickRow row = iterator.next();
                GroupKey key = new GroupKey(row, groupByColumns);
                JQuickRow aggRow = partialResults.computeIfAbsent(key, k -> {
                    JQuickRow newRow = new JQuickRow();
                    for (String col : groupByColumns) {
                        newRow.put(col, row.get(col));
                    }
                    return newRow;
                });
                // 局部聚合
                for (JQuickFunctionProvider<?, ?> provider : broadcastAggProviders) {
                    String targetField = provider.getTargetField();
                    Object currentValue = aggRow.get(targetField);
                    @SuppressWarnings("unchecked")
                    JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                            (JQuickFunctionProvider<JQuickRow, Object>) provider;
                    Object newValue = typedProvider.apply(row);

                    if (provider instanceof SumProvider) {
                        double sum = toDouble(currentValue) + toDouble(newValue);
                        aggRow.put(targetField, sum);
                    } else if (provider instanceof CountProvider) {
                        long count = toLong(currentValue) + 1;
                        aggRow.put(targetField, count);
                    } else {
                        aggRow.put(targetField, newValue);
                    }
                }
            }

            return partialResults.values().iterator();
        });

        // 第二阶段：全局 Shuffle 聚合（按 GroupKey 分组）
        JavaRDD<JQuickRow> finalAggRDD = partialAggRDD
                .mapToPair(row -> {
                    GroupKey key = new GroupKey(row, groupByColumns);
                    return new scala.Tuple2<>(key, row);
                })
                .reduceByKey((row1, row2) -> {
                    JQuickRow result = new JQuickRow(row1);
                    for (JQuickFunctionProvider<?, ?> provider : broadcastAggProviders) {
                        String targetField = provider.getTargetField();
                        Object val1 = row1.get(targetField);
                        Object val2 = row2.get(targetField);

                        if (provider instanceof SumProvider) {
                            result.put(targetField, toDouble(val1) + toDouble(val2));
                        } else if (provider instanceof CountProvider) {
                            result.put(targetField, toLong(val1) + toLong(val2));
                        }
                    }
                    return result;
                })
                .map(tuple -> tuple._2);

        // 收集结果
        List<JQuickRow> resultRows = finalAggRDD.collect();

        // 构建列元数据
        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (String col : groupByColumns) {
            columns.add(new JQuickColumnMeta(col, Object.class, "group_by"));
        }
        for (JQuickFunctionProvider<?, ?> provider : aggProviders) {
            columns.add(new JQuickColumnMeta(provider.getTargetField(), provider.getTargetClass(), "aggregate"));
        }

        return new JQuickDataSet(columns, resultRows);
    }

    /**
     * 从 Spark DataFrame 转换为 JQuickDataSet
     */
    public JQuickDataSet fromDataFrame(Dataset<Row> df) {
        List<JQuickColumnMeta> columns = Arrays.stream(df.columns())
                .map(name -> new JQuickColumnMeta(name,
                        df.schema().apply(name).dataType().getClass(),
                        "spark"))
                .collect(Collectors.toList());

        List<JQuickRow> rows = df.collectAsList().stream()
                .map(row -> {
                    JQuickRow jrow = new JQuickRow();
                    for (String name : df.columns()) {
                        jrow.put(name, row.getAs(name));
                    }
                    return jrow;
                })
                .collect(Collectors.toList());

        return new JQuickDataSet(columns, rows);
    }

    /**
     * 将 JQuickDataSet 转换为 Spark DataFrame
     */
    public Dataset<Row> toDataFrame(JQuickDataSet dataSet) {
        // 创建 Spark Row 的 RDD
        JavaRDD<Row> rowRDD = toRDD(dataSet).map(row -> {
            List<Object> values = new ArrayList<>();
            for (JQuickColumnMeta col : dataSet.getColumns()) {
                values.add(row.get(col.getName()));
            }
            return org.apache.spark.sql.RowFactory.create(values.toArray());
        });

        // 创建 Schema
        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType();
        for (JQuickColumnMeta col : dataSet.getColumns()) {
            schema = schema.add(col.getName(), sparkTypeOf(col.getType()), true);
        }

        return spark.createDataFrame(rowRDD, schema);
    }

    // 工具方法
    private double toDouble(Object value) {
        if (value == null) return 0.0;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return 0.0;
    }

    private long toLong(Object value) {
        if (value == null) return 0L;
        if (value instanceof Number) return ((Number) value).longValue();
        return 0L;
    }

    private org.apache.spark.sql.types.DataType sparkTypeOf(Class<?> clazz) {
        if (clazz == Integer.class) return org.apache.spark.sql.types.DataTypes.IntegerType;
        if (clazz == Long.class) return org.apache.spark.sql.types.DataTypes.LongType;
        if (clazz == Double.class) return org.apache.spark.sql.types.DataTypes.DoubleType;
        if (clazz == String.class) return org.apache.spark.sql.types.DataTypes.StringType;
        if (clazz == Boolean.class) return org.apache.spark.sql.types.DataTypes.BooleanType;
        return org.apache.spark.sql.types.DataTypes.StringType;
    }

    /**
     * 分组键（必须 Serializable）
     */
    private static class GroupKey implements Serializable {
        private final List<Object> values;

        GroupKey(JQuickRow row, List<String> columns) {
            this.values = columns.stream().map(row::get).collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupKey that = (GroupKey) o;
            return Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(values);
        }
    }
}
