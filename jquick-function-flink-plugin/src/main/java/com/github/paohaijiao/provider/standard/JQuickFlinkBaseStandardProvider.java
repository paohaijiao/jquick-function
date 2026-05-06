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
package com.github.paohaijiao.provider.standard;

import com.github.paohaijiao.extra.StandardProviderDropColumnsMapFunction;
import com.github.paohaijiao.extra.StandardProviderMapFunction;
import com.github.paohaijiao.extra.StandardProviderScalarFunction;
import com.github.paohaijiao.provider.JStandardProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Flink 抽象标准转换器基类 - 支持 Flink UDF
 *
 * <p>子类只需实现 {@link #transform(List)} 方法即可
 * <p>自动支持本地模式、Flink DataSet 和 Flink Table API
 *
 * @param <R> 输出类型
 */
public abstract class JQuickFlinkBaseStandardProvider<R> implements JStandardProvider<Object, R>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 依赖的源字段列表
     */
    protected final List<String> dependentColumns;

    /**
     * 输出的新字段名称
     */
    protected final String outputColumnName;

    /**
     * 临时 UDF 名称
     */
    private transient String tempUdfName;

    public JQuickFlinkBaseStandardProvider(List<String> dependentColumns, String outputColumnName) {
        if (dependentColumns == null || dependentColumns.isEmpty()) {
            throw new IllegalArgumentException("dependentColumns cannot be null or empty");
        }
        this.dependentColumns = new ArrayList<>(dependentColumns);
        this.outputColumnName = outputColumnName;
    }

    public JQuickFlinkBaseStandardProvider(String dependentColumn, String outputColumnName) {
        this(Collections.singletonList(dependentColumn), outputColumnName);
    }

    // ==================== 本地模式核心方法 ====================

    /**
     * DataSet 链式转换
     */
    @SafeVarargs
    public static DataSet<JQuickRow> transformChain(DataSet<JQuickRow> dataSet,
                                                    JQuickFlinkBaseStandardProvider<?>... providers) {
        DataSet<JQuickRow> result = dataSet;
        for (JQuickFlinkBaseStandardProvider<?> provider : providers) {
            result = provider.transform(result);
        }
        return result;
    }

    /**
     * JQuickDataSet 链式转换
     */
    @SafeVarargs
    public static JQuickDataSet transformChain(JQuickDataSet dataSet,
                                               JQuickFlinkBaseStandardProvider<?>... providers) {
        JQuickDataSet result = dataSet;
        for (JQuickFlinkBaseStandardProvider<?> provider : providers) {
            result = provider.transform(result);
        }
        return result;
    }

    @Override
    public R apply(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return onEmptyValues();
        }
        return transform(values);
    }

    /**
     * 核心转换方法 - 子类必须实现
     *
     * @param values 依赖字段的值列表，顺序与 dependentColumns 一致
     * @return 转换后的值
     */
    protected abstract R transform(List<Object> values);

    /**
     * 当输入值为 null 或空时的处理策略（子类可覆盖）
     */
    protected R onEmptyValues() {
        return null;
    }

    @Override
    public List<String> getDependentColumns() {
        return dependentColumns;
    }

    @Override
    public String getOutputColumnName() {
        return outputColumnName;
    }

    @Override
    public boolean skipOnNull() {
        return true;
    }

    /**
     * 转换为 Flink MapFunction
     */
    public MapFunction<JQuickRow, JQuickRow> toMapFunction() {
        return new StandardProviderMapFunction<>(this);
    }

    /**
     * 应用到 Flink DataSet
     */
    public DataSet<JQuickRow> transform(DataSet<JQuickRow> dataSet) {
        return dataSet.map(toMapFunction());
    }

    /**
     * 应用到 Flink DataSet，并控制是否保留原始列
     */
    public DataSet<JQuickRow> transform(DataSet<JQuickRow> dataSet, boolean keepOriginalColumns) {
        if (keepOriginalColumns) {
            return dataSet.map(toMapFunction());
        } else {
            return dataSet.map(new StandardProviderDropColumnsMapFunction<>(this));
        }
    }

    /**
     * 转换为 Flink ScalarFunction（用于 Table API）
     */
    public ScalarFunction toScalarFunction() {
        return new StandardProviderScalarFunction(this);
    }

    /**
     * 获取或创建临时 UDF 名称
     */
    private String getOrCreateTempUdfName(StreamTableEnvironment tableEnv) {
        if (tempUdfName == null) {
            tempUdfName = "temp_udf_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().replace("-", "");
            tableEnv.createTemporarySystemFunction(tempUdfName, toScalarFunction());
        }
        return tempUdfName;
    }

    /**
     * 注册为 Flink UDF（用于 SQL）
     *
     * @param tableEnv TableEnvironment
     * @param udfName  UDF 注册名称
     */
    public void registerUDF(StreamTableEnvironment tableEnv, String udfName) {
        tableEnv.createTemporarySystemFunction(udfName, toScalarFunction());
    }

    /**
     * 应用到 Flink Table（Table API）
     * 通过注册临时 UDF 并调用
     *
     * @param table    原始 Table
     * @param tableEnv TableEnvironment
     * @return 添加新列后的 Table
     */
    public Table transform(Table table, StreamTableEnvironment tableEnv) {
        String udfName = getOrCreateTempUdfName(tableEnv);

        // 构建列名数组
        String[] columnNames = dependentColumns.toArray(new String[0]);

        // 构建列表达式数组
        Expression[] columnExprs = new Expression[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            columnExprs[i] = $(columnNames[i]);
        }

        // 使用 call 函数调用 UDF
        return table.addColumns(
                call(udfName, columnExprs).as(outputColumnName)
        );
    }

    /**
     * 应用到 Flink Table（需要先注册 UDF）
     *
     * @param table   原始 Table
     * @param udfName 已注册的 UDF 名称
     * @return 添加新列后的 Table
     */
    public Table transformWithRegisteredUDF(Table table, String udfName) {
        switch (dependentColumns.size()) {
            case 1:
                return table.addColumns(
                        call(udfName, $(dependentColumns.get(0))).as(outputColumnName)
                );
            case 2:
                return table.addColumns(
                        call(udfName, $(dependentColumns.get(0)), $(dependentColumns.get(1))).as(outputColumnName)
                );
            case 3:
                return table.addColumns(
                        call(udfName, $(dependentColumns.get(0)), $(dependentColumns.get(1)), $(dependentColumns.get(2))).as(outputColumnName)
                );
            case 4:
                return table.addColumns(
                        call(udfName, $(dependentColumns.get(0)), $(dependentColumns.get(1)), $(dependentColumns.get(2)), $(dependentColumns.get(3))).as(outputColumnName)
                );
            default:
                // 超过4个参数，使用数组方式
                Expression[] columnExprs = new Expression[dependentColumns.size()];
                for (int i = 0; i < dependentColumns.size(); i++) {
                    columnExprs[i] = $(dependentColumns.get(i));
                }
                return table.addColumns(
                        call(udfName, columnExprs).as(outputColumnName)
                );
        }
    }

    // ==================== 辅助方法 ====================

    /**
     * 应用到 JQuickDataSet（本地批量处理）
     */
    public JQuickDataSet transform(JQuickDataSet dataSet) {
        if (dataSet == null || dataSet.isEmpty()) {
            return dataSet;
        }

        validateColumns(dataSet);

        List<JQuickRow> newRows = new ArrayList<>();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickRow newRow = new JQuickRow(row);
            List<Object> values = extractValues(row);
            R result = apply(values);
            newRow.put(outputColumnName, result);
            newRows.add(newRow);
        }

        List<JQuickColumnMeta> newColumns = new ArrayList<>(dataSet.getColumns());
        newColumns.add(new JQuickColumnMeta(outputColumnName, getOutputClass(), "flink_transform"));

        return new JQuickDataSet(newColumns, newRows);
    }

    /**
     * 应用到 JQuickDataSet，并控制是否保留原始列
     */
    public JQuickDataSet transform(JQuickDataSet dataSet, boolean keepOriginalColumns) {
        JQuickDataSet result = transform(dataSet);
        if (!keepOriginalColumns) {
            List<JQuickRow> newRows = new ArrayList<>();
            for (JQuickRow row : result.getRows()) {
                JQuickRow newRow = new JQuickRow(row);
                for (String col : dependentColumns) {
                    if (!col.equals(outputColumnName)) {
                        newRow.remove(col);
                    }
                }
                newRows.add(newRow);
            }

            List<JQuickColumnMeta> newColumns = result.getColumns().stream()
                    .filter(col -> !dependentColumns.contains(col.getName()) || col.getName().equals(outputColumnName))
                    .collect(Collectors.toList());

            return new JQuickDataSet(newColumns, newRows);
        }
        return result;
    }

    private List<Object> extractValues(JQuickRow row) {
        List<Object> values = new ArrayList<>();
        for (String col : dependentColumns) {
            values.add(row.get(col));
        }
        return values;
    }

    private void validateColumns(JQuickDataSet dataSet) {
        List<String> availableColumns = dataSet.getColumnNames();
        for (String col : dependentColumns) {
            if (!availableColumns.contains(col)) {
                throw new IllegalArgumentException(
                        String.format("Dependent column '%s' not found in dataset. Available columns: %s",
                                col, availableColumns)
                );
            }
        }
    }

    public abstract Class<R> getOutputClass();
}