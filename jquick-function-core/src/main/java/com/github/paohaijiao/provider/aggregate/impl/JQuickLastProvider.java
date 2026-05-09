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
import com.github.paohaijiao.domain.impl.JQuickLastAggregator;
import com.github.paohaijiao.provider.aggregate.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * LAST 聚合提供者 - 获取最后一个非空值
 *
 * 使用 LastAggregator 作为内部累加器
 *
 * 使用示例：
 * <pre>
 * // 创建 LastProvider
 * LastProvider<String> lastNameProvider = new LastProvider<>("name", "lastName", String.class);
 * LastProvider<Double> lastSalaryProvider = new LastProvider<>("salary", "lastSalary", Double.class);
 *
 * // 获取初始累加器
 * LastAggregator<String> aggregator = lastNameProvider.getInitialValue();
 *
 * // 累加每一行的值
 * for (JQuickRow row : dataSet.getRows()) {
 *     LastAggregator<String> value = lastNameProvider.apply(row);
 *     aggregator = lastNameProvider.accumulate(aggregator, value);
 * }
 *
 * // 获取最终的最后一个值
 * String lastName = lastNameProvider.extractResult(aggregator);
 * </pre>
 *
 * @param <T> 值类型
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickLastProvider<T> extends JQuickAbstractAggregationProvider<JQuickLastAggregator<T>> {

    private final Class<T> valueClass;
    public JQuickLastProvider(String sourceColumn, String targetField) {
        this(sourceColumn, targetField, (Class) JQuickLastAggregator.class);
    }
    /**
     * 构造函数
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     * @param valueClass   值的类型
     */
    public JQuickLastProvider(String sourceColumn, String targetField, Class<T> valueClass) {
        super(sourceColumn, targetField, (Class) JQuickLastAggregator.class);
        this.valueClass = valueClass;
    }

    @Override
    public JQuickLastAggregator<T> getInitialValue() {
        return new JQuickLastAggregator<>();
    }

    @Override
    public String getName() {
        return JQuickAggregateProviderMethodConstants.LAST;
    }

    @Override
    public JQuickLastAggregator<T> apply(JQuickRow row) {
        T value = row.getAs(sourceColumn, valueClass);
        JQuickLastAggregator<T> aggregator = new JQuickLastAggregator<>();
        aggregator.add(value);
        return aggregator;
    }

    @Override
    public JQuickLastAggregator<T> accumulate(JQuickLastAggregator<T> current, JQuickLastAggregator<T> next) {
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
    public JQuickLastAggregator<T> merge(JQuickLastAggregator<T> a, JQuickLastAggregator<T> b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的最后一个值
     *
     * @param aggregator 聚合累加器
     * @return 最后一个值，无数据时返回 null
     */
    public T extractResult(JQuickLastAggregator<T> aggregator) {
        return aggregator == null ? null : aggregator.getLast();
    }

    /**
     * 便捷方法：直接获取数据集某一列的最后一个值
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @param valueClass   值类型
     * @return 最后一个值，无数据时返回 null
     */
    public static <R> R getLast(JQuickDataSet dataSet, String sourceColumn, Class<R> valueClass) {
        if (dataSet == null || dataSet.getRows().isEmpty()) {
            return null;
        }
        JQuickLastProvider<R> provider = new JQuickLastProvider<>(sourceColumn, "last", valueClass);
        JQuickLastAggregator<R> result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickLastAggregator<R> value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }
}