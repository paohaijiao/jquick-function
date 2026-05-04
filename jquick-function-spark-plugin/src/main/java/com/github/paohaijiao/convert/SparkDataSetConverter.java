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
package com.github.paohaijiao.convert;

import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Spark数据集转换器
 * 负责将JQuickDataSet转换为Spark DataFrame，以及反向转换
 */
public class SparkDataSetConverter {

    /**
     * 将JQuickDataSet转换为Spark DataFrame
     */
    public static Dataset<Row> toSparkDataFrame(JQuickDataSet dataset, SparkSession spark) {
        if (dataset == null || dataset.isEmpty()) {
            return createEmptyDataFrame(spark, dataset);
        }
        StructType schema = buildSchema(dataset.getColumns());// 构建Schema
        List<Row> rows = dataset.getRows().stream()
                .map(row -> convertToSparkRow(row, dataset.getColumns()))
                .collect(Collectors.toList());// 转换数据行
        return spark.createDataFrame(rows, schema);// 创建DataFrame
    }

    /**
     * 将Spark DataFrame转换为JQuickDataSet
     */
    public static JQuickDataSet fromSparkDataFrame(Dataset<Row> df, String datasetName) {
        if (df == null) {
            return new JQuickDataSet(Collections.emptyList(), Collections.emptyList());
        }
        List<JQuickColumnMeta> columns = extractColumnMetadata(df);// 获取列元数据
        List<Row> rows = df.collectAsList();
        List<JQuickRow> jquickRows = rows.stream()
                .map(row -> convertToJQuickRow(row, columns))
                .collect(Collectors.toList());
        return new JQuickDataSet(columns, jquickRows);
    }

    /**
     * 构建Spark Schema
     */
    private static StructType buildSchema(List<JQuickColumnMeta> columns) {
        StructType schema = new StructType();
        for (JQuickColumnMeta col : columns) {
            DataType sparkType = toSparkDataType(col.getType());
            schema = schema.add(col.getName(), sparkType, true);
        }
        return schema;
    }

    /**
     * 将Java类型转换为Spark SQL类型
     */
    private static DataType toSparkDataType(Class<?> javaType) {
        if (javaType == String.class) return DataTypes.StringType;
        if (javaType == Integer.class || javaType == int.class) return DataTypes.IntegerType;
        if (javaType == Long.class || javaType == long.class) return DataTypes.LongType;
        if (javaType == Double.class || javaType == double.class) return DataTypes.DoubleType;
        if (javaType == Float.class || javaType == float.class) return DataTypes.FloatType;
        if (javaType == Boolean.class || javaType == boolean.class) return DataTypes.BooleanType;
        if (javaType == java.util.Date.class || javaType == Date.class) return DataTypes.DateType;
        if (javaType == Timestamp.class) return DataTypes.TimestampType;
        return DataTypes.StringType;
    }

    /**
     * 将JQuickRow转换为Spark Row
     */
    private static Row convertToSparkRow(JQuickRow row, List<JQuickColumnMeta> columns) {
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i).getName();
            Object value = row.get(colName);
            values[i] = convertValueForSpark(value);// 类型转换处理
        }
        return RowFactory.create(values);
    }

    /**
     * 转换值为Spark兼容类型
     */
    private static Object convertValueForSpark(Object value) {
        if (value == null) return null;
        if (value instanceof java.util.Date && !(value instanceof Date)) {
            return new Date(((java.util.Date) value).getTime());
        }
        return value;
    }

    /**
     * 提取列元数据
     */
    private static List<JQuickColumnMeta> extractColumnMetadata(Dataset<Row> df) {
        StructType schema = df.schema();
        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (StructField field : schema.fields()) {
            Class<?> javaType = toJavaType(field.dataType());
            columns.add(new JQuickColumnMeta(field.name(), javaType, "spark"));
        }
        return columns;
    }

    /**
     * 将Spark类型转换为Java类型
     */
    private static Class<?> toJavaType(DataType sparkType) {
        if (sparkType == DataTypes.StringType) return String.class;
        if (sparkType == DataTypes.IntegerType) return Integer.class;
        if (sparkType == DataTypes.LongType) return Long.class;
        if (sparkType == DataTypes.DoubleType) return Double.class;
        if (sparkType == DataTypes.FloatType) return Float.class;
        if (sparkType == DataTypes.BooleanType) return Boolean.class;
        if (sparkType == DataTypes.DateType) return Date.class;
        if (sparkType == DataTypes.TimestampType) return Timestamp.class;
        return String.class;
    }

    /**
     * 将Spark Row转换为JQuickRow
     */
    private static JQuickRow convertToJQuickRow(Row row, List<JQuickColumnMeta> columns) {
        JQuickRow jquickRow = new JQuickRow();
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i).getName();
            Object value = row.get(i);
            jquickRow.put(colName, value);
        }
        return jquickRow;
    }

    /**
     * 创建空的DataFrame
     */
    private static Dataset<Row> createEmptyDataFrame(SparkSession spark, JQuickDataSet dataset) {
        if (dataset == null || dataset.getColumns().isEmpty()) {
            return spark.emptyDataFrame();
        }
        StructType schema = buildSchema(dataset.getColumns());
        return spark.createDataFrame(Collections.emptyList(), schema);
    }
}