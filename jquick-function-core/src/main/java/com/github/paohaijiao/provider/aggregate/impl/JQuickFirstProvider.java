package com.github.paohaijiao.provider.aggregate.impl;
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

import com.github.paohaijiao.core.constant.JQuickAggregateProviderMethodConstants;
import com.github.paohaijiao.domain.impl.JQuickFirstAggregator;
import com.github.paohaijiao.provider.aggregate.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * FIRST 聚合提供者 - 获取第一个非空值
 *
 * 使用 FirstAggregator 作为内部累加器
 *
 * 使用示例：
 * <pre>
 * // 创建 FirstProvider
 * FirstProvider firstNameProvider = new FirstProvider("name", "firstName");
 * FirstProvider firstSalaryProvider = new FirstProvider("salary", "firstSalary");
 *
 * // 获取初始累加器
 * FirstAggregator<String> aggregator = firstNameProvider.getInitialValue();
 *
 * // 累加每一行的值
 * for (JQuickRow row : dataSet.getRows()) {
 *     FirstAggregator<String> value = firstNameProvider.apply(row);
 *     aggregator = firstNameProvider.accumulate(aggregator, value);
 * }
 *
 * // 获取最终的第一个值
 * String firstName = firstNameProvider.extractResult(aggregator);
 * </pre>
 *
 * @param <T> 值类型
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class JQuickFirstProvider<T> extends JQuickAbstractAggregationProvider<JQuickFirstAggregator<T>> {

    private final Class<T> valueClass;

    public JQuickFirstProvider(String sourceColumn, String targetField) {
        this(sourceColumn, targetField, (Class) JQuickFirstAggregator.class);
    }
    /**
     * 构造函数
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     * @param valueClass   值的类型
     */
    public JQuickFirstProvider(String sourceColumn, String targetField, Class<T> valueClass) {
        super(sourceColumn, targetField, (Class) JQuickFirstAggregator.class);
        this.valueClass = valueClass;
    }

    @Override
    public JQuickFirstAggregator<T> getInitialValue() {
        return new JQuickFirstAggregator<>();
    }

    @Override
    public String getName() {
        return JQuickAggregateProviderMethodConstants.FIRST;
    }

    @Override
    public JQuickFirstAggregator<T> apply(JQuickRow row) {
        T value = row.getAs(sourceColumn, valueClass);
        JQuickFirstAggregator<T> aggregator = new JQuickFirstAggregator<>();
        aggregator.add(value);
        return aggregator;
    }

    @Override
    public JQuickFirstAggregator<T> accumulate(JQuickFirstAggregator<T> current, JQuickFirstAggregator<T> next) {
        if (current == null) {
            return next;
        }
        if (next == null) {
            return current;
        }
        current.merge(next);
        return current;
    }

    @Override
    public JQuickFirstAggregator<T> merge(JQuickFirstAggregator<T> a, JQuickFirstAggregator<T> b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的第一个值
     *
     * @param aggregator 聚合累加器
     * @return 第一个值，无数据时返回 null
     */
    public T extractResult(JQuickFirstAggregator<T> aggregator) {
        return aggregator == null ? null : aggregator.getFirst();
    }

    /**
     * 判断是否已有值
     */
    public boolean isFirstSet(JQuickFirstAggregator<T> aggregator) {
        return aggregator != null && aggregator.isFirstSet();
    }

    /**
     * 便捷方法：直接获取数据集某一列的第一个值
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @param valueClass   值类型
     * @return 第一个值，无数据时返回 null
     */
    public static <T> T getFirst(JQuickDataSet dataSet, String sourceColumn, Class<T> valueClass) {
        if (dataSet == null || dataSet.getRows().isEmpty()) {
            return null;
        }
        JQuickFirstProvider<T> provider = new JQuickFirstProvider<>(sourceColumn, "first", valueClass);
        JQuickFirstAggregator<T> result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickFirstAggregator<T> value = provider.apply(row);
            result = provider.accumulate(result, value);
            if (result.isFirstSet()) {
                break; // 已找到第一个值，可以提前结束
            }
        }
        return provider.extractResult(result);
    }
}
