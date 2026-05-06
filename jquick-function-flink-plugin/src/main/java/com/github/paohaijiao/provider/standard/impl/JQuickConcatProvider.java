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
import com.github.paohaijiao.provider.standard.JQuickFlinkBaseStandardProvider;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 将多个字段拼接成字符串（Flink 版本）
 *
 * <p>使用示例：
 * <pre>
 * // 单字段拼接
 * ConcatProvider provider = new ConcatProvider("name", "name_str", "", false);
 *
 * // 多字段拼接
 * ConcatProvider provider = new ConcatProvider(
 *     Arrays.asList("first_name", "last_name"),
 *     "full_name",
 *     " "
 * );
 *
 * // Flink DataSet
 * DataSet&lt;JQuickRow&gt; result = provider.transform(dataSet);
 *
 * // Flink Table
 * Table result = provider.transform(table);
 * </pre>
 */
public class JQuickConcatProvider extends JQuickFlinkBaseStandardProvider<String> {

    private final String delimiter;
    private final boolean skipNull;
    private final String nullReplacement;

    /**
     * 构造函数 - 多字段拼接
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     * @param delimiter        分隔符
     */
    public JQuickConcatProvider(List<String> dependentColumns, String outputColumnName, String delimiter) {
        this(dependentColumns, outputColumnName, delimiter, true, null);
    }

    /**
     * 构造函数 - 多字段拼接
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     * @param delimiter        分隔符
     * @param skipNull         是否跳过 null 值
     */
    public JQuickConcatProvider(List<String> dependentColumns, String outputColumnName, String delimiter, boolean skipNull) {
        this(dependentColumns, outputColumnName, delimiter, skipNull, null);
    }

    /**
     * 构造函数 - 多字段拼接（完整参数）
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     * @param delimiter        分隔符
     * @param skipNull         是否跳过 null 值
     * @param nullReplacement  null 值的替换字符串（skipNull 为 false 时生效）
     */
    public JQuickConcatProvider(List<String> dependentColumns, String outputColumnName,
                                String delimiter, boolean skipNull, String nullReplacement) {
        super(dependentColumns, outputColumnName);
        this.delimiter = delimiter != null ? delimiter : "";
        this.skipNull = skipNull;
        this.nullReplacement = nullReplacement != null ? nullReplacement : "null";
    }

    /**
     * 构造函数 - 双字段拼接（便捷方法）
     *
     * @param col1             第一个字段名
     * @param col2             第二个字段名
     * @param outputColumnName 输出的新字段名称
     * @param delimiter        分隔符
     */
    public JQuickConcatProvider(String col1, String col2, String outputColumnName, String delimiter) {
        this(java.util.Arrays.asList(col1, col2), outputColumnName, delimiter, true, null);
    }

    /**
     * 构造函数 - 单字段（直接返回字符串）
     *
     * @param dependentColumn  依赖的源字段
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickConcatProvider(String dependentColumn, String outputColumnName) {
        this(java.util.Collections.singletonList(dependentColumn), outputColumnName, "", false, null);
    }

    @Override
    protected String transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        if (skipNull) {
            // 跳过 null 值
            return values.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.joining(delimiter));
        } else {
            // 保留 null 值，用替换字符串代替
            return values.stream()
                    .map(v -> v != null ? v.toString() : nullReplacement)
                    .collect(Collectors.joining(delimiter));
        }
    }

    @Override
    public Class<String> getOutputClass() {
        return String.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return null;
    }
}
