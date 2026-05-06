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


import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.JStandardProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 抽象标准转换器基类 - 输入 List&lt;Object&gt;（多字段），输出单个值 R
 *
 * <p>子类只需实现 {@link #transform(List)} 方法即可
 * <p>支持本地模式和 JQuickDataSet 转换
 * <p>支持 transformChain 链式调用
 *
 * @param <R> 输出类型
 */
public abstract class JQuickBaseStandardProvider<R> implements JStandardProvider<Object, R> {

    /** 依赖的源字段列表 */
    protected final List<String> dependentColumns;

    /** 输出的新字段名称 */
    protected final String outputColumnName;

    /**
     * 构造函数 - 多依赖字段
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickBaseStandardProvider(List<String> dependentColumns, String outputColumnName) {
        if (dependentColumns == null || dependentColumns.isEmpty()) {
            throw new IllegalArgumentException("dependentColumns cannot be null or empty");
        }
        this.dependentColumns = new ArrayList<>(dependentColumns);
        this.outputColumnName = outputColumnName;
    }

    /**
     * 构造函数 - 单依赖字段
     *
     * @param dependentColumn 依赖的源字段
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickBaseStandardProvider(String dependentColumn, String outputColumnName) {
        this(Collections.singletonList(dependentColumn), outputColumnName);
    }

    @Override
    public R apply(List<Object> values) {
        // 输入 List<Object>（多字段值），输出单个值 R
        if (values == null || values.isEmpty()) {
            return onEmptyValues();
        }
        return transform(values);
    }

    /**
     * 核心转换方法 - 将输入值列表转换为单个输出值（子类必须实现）
     *
     * @param values 依赖字段的值列表，顺序与 dependentColumns 一致
     * @return 转换后的值
     */
    protected abstract R transform(List<Object> values);

    /**
     * 当输入值为 null 或空时的处理策略
     * 子类可覆盖此方法自定义行为
     *
     * @return 默认返回 null
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
     * 从行中提取依赖字段的值
     */
    private List<Object> extractValues(JQuickRow row) {
        List<Object> values = new ArrayList<>();
        for (String col : dependentColumns) {
            values.add(row.get(col));
        }
        return values;
    }

    /**
     * 验证所有依赖字段是否存在
     */
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

    /**
     * 将当前转换器应用到 JQuickDataSet
     *
     * @param dataSet 原始数据集
     * @return 添加新列后的数据集
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
        newColumns.add(new JQuickColumnMeta(outputColumnName, getOutputClass(), "standard_transform"));
        return new JQuickDataSet(newColumns, newRows);
    }

    /**
     * 将当前转换器应用到 JQuickDataSet，并控制是否保留原始列
     *
     * @param dataSet 原始数据集
     * @param keepOriginalColumns 是否保留原始依赖列
     * @return 转换后的数据集
     */
    public JQuickDataSet transform(JQuickDataSet dataSet, boolean keepOriginalColumns) {
        JQuickDataSet result = transform(dataSet);
        if (!keepOriginalColumns) {
            // 删除原始依赖列
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
            // 更新列元数据
            List<JQuickColumnMeta> newColumns = result.getColumns().stream()
                    .filter(col -> !dependentColumns.contains(col.getName()) || col.getName().equals(outputColumnName))
                    .collect(Collectors.toList());

            return new JQuickDataSet(newColumns, newRows);
        }
        return result;
    }

    /**
     * 对单个 JQuickRow 应用转换
     *
     * @param row 原始行
     * @return 添加新列后的行
     */
    public JQuickRow transformRow(JQuickRow row) {
        JQuickRow newRow = new JQuickRow(row);
        List<Object> values = extractValues(row);
        R result = apply(values);
        newRow.put(outputColumnName, result);
        return newRow;
    }

    /**
     * 对单个 JQuickRow 应用转换，并控制是否保留原始列
     *
     * @param row 原始行
     * @param keepOriginalColumns 是否保留原始依赖列
     * @return 添加新列后的行
     */
    public JQuickRow transformRow(JQuickRow row, boolean keepOriginalColumns) {
        JQuickRow newRow = transformRow(row);
        if (!keepOriginalColumns) {
            for (String col : dependentColumns) {
                if (!col.equals(outputColumnName)) {
                    newRow.remove(col);
                }
            }
        }
        return newRow;
    }
    /**
     * 批量应用到 JQuickDataSet（多个 Provider 链式调用）
     *
     * @param dataSet 原始数据集
     * @param providers Provider 列表
     * @return 转换后的数据集
     */
    @SafeVarargs
    public static JQuickDataSet transformChain(JQuickDataSet dataSet, JQuickBaseStandardProvider<?>... providers) {
        if (providers == null || providers.length == 0) {
            return dataSet;
        }
        JQuickDataSet result = dataSet;
        for (JQuickBaseStandardProvider<?> provider : providers) {
            result = provider.transform(result);
        }
        return result;
    }

    /**
     * 批量应用到 JQuickDataSet（多个 Provider 链式调用），并控制是否保留原始列
     *
     * @param dataSet 原始数据集
     * @param keepOriginalColumns 是否保留原始依赖列
     * @param providers Provider 列表
     * @return 转换后的数据集
     */
    @SafeVarargs
    public static JQuickDataSet transformChain(JQuickDataSet dataSet, boolean keepOriginalColumns, JQuickBaseStandardProvider<?>... providers) {
        if (providers == null || providers.length == 0) {
            return dataSet;
        }
        JQuickDataSet result = dataSet;
        for (JQuickBaseStandardProvider<?> provider : providers) {
            result = provider.transform(result, keepOriginalColumns);
        }
        return result;
    }

    /**
     * 批量应用到行列表（多个 Provider 链式调用）
     *
     * @param rows 原始行列表
     * @param providers Provider 列表
     * @return 转换后的行列表
     */
    @SafeVarargs
    public static List<JQuickRow> transformChainRows(List<JQuickRow> rows, JQuickBaseStandardProvider<?>... providers) {
        if (rows == null || rows.isEmpty()) {
            return rows;
        }
        if (providers == null || providers.length == 0) {
            return new ArrayList<>(rows);
        }

        List<JQuickRow> result = new ArrayList<>(rows);
        for (JQuickBaseStandardProvider<?> provider : providers) {
            List<JQuickRow> newRows = new ArrayList<>();
            for (JQuickRow row : result) {
                newRows.add(provider.transformRow(row));
            }
            result = newRows;
        }
        return result;
    }

    /**
     * 批量应用到行列表，并控制是否保留原始列
     *
     * @param rows 原始行列表
     * @param keepOriginalColumns 是否保留原始依赖列
     * @param providers Provider 列表
     * @return 转换后的行列表
     */
    @SafeVarargs
    public static List<JQuickRow> transformChainRows(List<JQuickRow> rows, boolean keepOriginalColumns, JQuickBaseStandardProvider<?>... providers) {
        if (rows == null || rows.isEmpty()) {
            return rows;
        }
        if (providers == null || providers.length == 0) {
            return new ArrayList<>(rows);
        }

        List<JQuickRow> result = new ArrayList<>(rows);
        for (JQuickBaseStandardProvider<?> provider : providers) {
            List<JQuickRow> newRows = new ArrayList<>();
            for (JQuickRow row : result) {
                newRows.add(provider.transformRow(row, keepOriginalColumns));
            }
            result = newRows;
        }
        return result;
    }

    /**
     * 获取输出类型的 Class（子类必须实现）
     */
    public abstract Class<R> getOutputClass();
}