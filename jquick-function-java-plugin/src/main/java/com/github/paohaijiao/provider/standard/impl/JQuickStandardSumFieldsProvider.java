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
package com.github.paohaijiao.provider.standard.impl;


import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.standard.JQuickBaseStandardProvider;

import java.util.List;

/**
 * 将多个数值字段求和
 *
 * <p>支持的类型：
 * <ul>
 *   <li>Number 类型（Integer, Long, Double, Float 等）</li>
 *   <li>String 类型（会尝试解析为 Double）</li>
 *   <li>Boolean 类型（true=1.0, false=0.0）</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>
 * // 多字段求和
 * SumFieldsProvider provider = new SumFieldsProvider(
 *     Arrays.asList("price", "tax", "shipping"),
 *     "total"
 * );
 *
 * // 链式调用
 * JQuickDataSet result = JQuickBaseStandardProvider.transformChain(
 *     dataSet,
 *     new ToDoubleProvider("price", "price_double"),
 *     new SumFieldsProvider(Arrays.asList("price_double", "tax"), "total")
 * );
 * </pre>
 */
public class JQuickStandardSumFieldsProvider extends JQuickBaseStandardProvider<Double> {

    private final boolean skipNull;
    private final double defaultValue;

    /**
     * 构造函数 - 多字段求和
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickStandardSumFieldsProvider(List<String> dependentColumns, String outputColumnName) {
        this(dependentColumns, outputColumnName, true, 0.0);
    }

    /**
     * 构造函数 - 双字段求和（便捷方法）
     *
     * @param col1 第一个字段名
     * @param col2 第二个字段名
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickStandardSumFieldsProvider(String col1, String col2, String outputColumnName) {
        this(java.util.Arrays.asList(col1, col2), outputColumnName, true, 0.0);
    }

    /**
     * 构造函数 - 三字段求和（便捷方法）
     *
     * @param col1 第一个字段名
     * @param col2 第二个字段名
     * @param col3 第三个字段名
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickStandardSumFieldsProvider(String col1, String col2, String col3, String outputColumnName) {
        this(java.util.Arrays.asList(col1, col2, col3), outputColumnName, true, 0.0);
    }

    /**
     * 构造函数 - 多字段求和
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     * @param skipNull 是否跳过 null 值（true: 跳过, false: 视为 0）
     */
    public JQuickStandardSumFieldsProvider(List<String> dependentColumns, String outputColumnName, boolean skipNull) {
        this(dependentColumns, outputColumnName, skipNull, 0.0);
    }

    /**
     * 构造函数 - 多字段求和（完整参数）
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     * @param skipNull 是否跳过 null 值
     * @param defaultValue 默认值（当 skipNull 为 false 且值为 null 时使用）
     */
    public JQuickStandardSumFieldsProvider(List<String> dependentColumns, String outputColumnName,
                                           boolean skipNull, double defaultValue) {
        super(dependentColumns, outputColumnName);
        this.skipNull = skipNull;
        this.defaultValue = defaultValue;
    }

    @Override
    protected Double transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        double sum = 0.0;
        boolean hasAny = false;

        for (Object value : values) {
            Double num = toDouble(value);

            if (num != null) {
                sum += num;
                hasAny = true;
            } else if (!skipNull) {
                // 不跳过 null，使用默认值
                sum += defaultValue;
                hasAny = true;
            }
            // skipNull 为 true 且 value 为 null 时，跳过不处理
        }

        return hasAny ? sum : null;
    }

    /**
     * 将值转换为 Double
     */
    private Double toDouble(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        if (value instanceof String) {
            String str = ((String) value).trim();
            if (str.isEmpty()) {
                return null;
            }
            try {
                return Double.parseDouble(str);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1.0 : 0.0;
        }

        return null;
    }

    @Override
    public Class<Double> getOutputClass() {
        return Double.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return null;
    }
}
