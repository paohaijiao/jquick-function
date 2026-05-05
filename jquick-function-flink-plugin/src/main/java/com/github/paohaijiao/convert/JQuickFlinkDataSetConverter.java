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

/**
 * packageName com.github.paohaijiao.convert
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */

import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink数据集转换器
 * 负责将JQuickDataSet转换为Flink DataSet/DataStream/Table
 */
public class JQuickFlinkDataSetConverter {

    /**
     * 将JQuickDataSet转换为Flink DataSet（批处理）
     */
    public static DataSet<Row> toDataSet(JQuickDataSet dataset, ExecutionEnvironment env) {
        if (dataset == null || dataset.isEmpty()) {
            return env.fromCollection(Collections.emptyList(), getRowTypeInfo(dataset));
        }
        List<Row> rows = convertToFlinkRows(dataset);
        return env.fromCollection(rows, getRowTypeInfo(dataset));
    }

    /**
     * 将JQuickDataSet转换为Flink DataStream（流处理）
     */
    public static DataStream<Row> toDataStream(JQuickDataSet dataset, StreamExecutionEnvironment env) {
        if (dataset == null || dataset.isEmpty()) {
            return env.fromCollection(Collections.emptyList(), getRowTypeInfo(dataset));
        }
        List<Row> rows = convertToFlinkRows(dataset);
        return env.fromCollection(rows, getRowTypeInfo(dataset));
    }

    /**
     * 将JQuickDataSet转换为Flink Table
     */
    public static Table toTable(JQuickDataSet dataset, StreamTableEnvironment tableEnv, String tableName) {
        DataStream<Row> dataStream = toDataStream(dataset, tableEnv.getStreamExecutionEnvironment());
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (JQuickColumnMeta column : dataset.getColumns()) {
            schemaBuilder.column(column.getName(), toFlinkDataType(column.getType()));
        }
        return tableEnv.fromDataStream(dataStream, schemaBuilder.build());
    }

    /**
     * 将Flink DataSet转换为JQuickDataSet
     */
    public static JQuickDataSet fromDataSet(DataSet<Row> dataSet, String datasetName) throws Exception {
        List<Row> rows = dataSet.collect();
        return convertToJQuickDataSet(rows, datasetName);
    }

    /**
     * 将Flink DataStream转换为JQuickDataSet（用于有限流）
     */
    public static JQuickDataSet fromDataStream(DataStream<Row> dataStream, String datasetName) throws Exception {
        // 注意：这仅适用于有限数据流
        List<Row> rows = new ArrayList<>();
        dataStream.executeAndCollect().forEachRemaining(rows::add);
        return convertToJQuickDataSet(rows, datasetName);
    }

    /**
     * 将Flink Table转换为JQuickDataSet
     */
    public static JQuickDataSet fromTable(Table table, String datasetName) throws Exception {
        // 转换为DataStream再收集
        StreamTableEnvironment tableEnv = (StreamTableEnvironment) table.getTableEnvironment();
        DataStream<Row> dataStream = tableEnv.toDataStream(table);
        return fromDataStream(dataStream, datasetName);
    }

    /**
     * 转换为Flink Row列表
     */
    private static List<Row> convertToFlinkRows(JQuickDataSet dataset) {
        List<Row> rows = new ArrayList<>();
        List<JQuickColumnMeta> columns = dataset.getColumns();

        for (JQuickRow jquickRow : dataset.getRows()) {
            Row row = new Row(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                String colName = columns.get(i).getName();
                row.setField(i, jquickRow.get(colName));
            }
            rows.add(row);
        }

        return rows;
    }

    /**
     * 获取类型信息
     */
    private static TypeInformation<Row> getRowTypeInfo(JQuickDataSet dataset) {
        if (dataset == null || dataset.getColumns().isEmpty()) {
            return Types.ROW_NAMED(new String[0], new TypeInformation<?>[0]);
        }

        String[] fieldNames = dataset.getColumns().stream()
                .map(JQuickColumnMeta::getName)
                .toArray(String[]::new);

        TypeInformation<?>[] fieldTypes = dataset.getColumns().stream()
                .map(col -> toFlinkTypeInfo(col.getType()))
                .toArray(TypeInformation<?>[]::new);

        return Types.ROW_NAMED(fieldNames, fieldTypes);
    }

    /**
     * 转换为Flink类型信息
     */
    private static TypeInformation<?> toFlinkTypeInfo(Class<?> javaType) {
        if (javaType == String.class) return Types.STRING;
        if (javaType == Integer.class || javaType == int.class) return Types.INT;
        if (javaType == Long.class || javaType == long.class) return Types.LONG;
        if (javaType == Double.class || javaType == double.class) return Types.DOUBLE;
        if (javaType == Float.class || javaType == float.class) return Types.FLOAT;
        if (javaType == Boolean.class || javaType == boolean.class) return Types.BOOLEAN;
        if (javaType == Date.class) return Types.SQL_DATE;
        return Types.STRING;
    }

    /**
     * 转换为Flink数据类型
     */
    private static DataTypes.DataType toFlinkDataType(Class<?> javaType) {
        if (javaType == String.class) return DataTypes.STRING();
        if (javaType == Integer.class || javaType == int.class) return DataTypes.INT();
        if (javaType == Long.class || javaType == long.class) return DataTypes.BIGINT();
        if (javaType == Double.class || javaType == double.class) return DataTypes.DOUBLE();
        if (javaType == Float.class || javaType == float.class) return DataTypes.FLOAT();
        if (javaType == Boolean.class || javaType == boolean.class) return DataTypes.BOOLEAN();
        if (javaType == Date.class) return DataTypes.DATE();
        return DataTypes.STRING();
    }

    /**
     * 转换为JQuickDataSet
     */
    private static JQuickDataSet convertToJQuickDataSet(List<Row> rows, String datasetName) {
        if (rows.isEmpty()) {
            return new JQuickDataSet(Collections.emptyList(), Collections.emptyList());
        }

        // 提取列信息
        Row firstRow = rows.get(0);
        int arity = firstRow.getArity();

        List<JQuickColumnMeta> columns = new ArrayList<>();
        for (int i = 0; i < arity; i++) {
            String colName = "col_" + i;
            Class<?> colType = firstRow.getField(i) != null ?
                    firstRow.getField(i).getClass() : Object.class;
            columns.add(new JQuickColumnMeta(colName, colType, "flink"));
        }
        List<JQuickRow> jquickRows = rows.stream()
                .map(row -> {
                    JQuickRow jrow = new JQuickRow();
                    for (int i = 0; i < arity; i++) {
                        String colName = columns.get(i).getName();
                        jrow.put(colName, row.getField(i));
                    }
                    return jrow;
                })
                .collect(Collectors.toList());

        return new JQuickDataSet(columns, jquickRows);
    }
}