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
import com.github.paohaijiao.domain.impl.JQuickAvgAggregator;
import com.github.paohaijiao.provider.aggregate.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * AVG 聚合提供者 - 计算平均值
 *
 * 使用 AvgAggregator 作为内部累加器，存储总和与计数
 *
 * 使用示例：
 * <pre>
 * // 创建 AvgProvider
 * AvgProvider avgProvider = new AvgProvider("salary", "avg_salary");
 *
 * // 获取初始累加器
 * AvgAggregator<Number> aggregator = avgProvider.getInitialValue();
 *
 * // 累加每一行的值
 * for (JQuickRow row : dataSet.getRows()) {
 *     AvgAggregator<Number> value = avgProvider.apply(row);
 *     aggregator = avgProvider.accumulate(aggregator, value);
 * }
 *
 * // 获取最终的平均值
 * Double avg = avgProvider.extractResult(aggregator);
 * </pre>
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickAvgProvider extends JQuickAbstractAggregationProvider<JQuickAvgAggregator<Number>> {
    @Override
    public String getName() {
        return JQuickAggregateProviderMethodConstants.AVG;
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    public JQuickAvgProvider(String sourceColumn, String targetField) {
        super(sourceColumn, targetField, (Class) JQuickAvgAggregator.class);
    }

    @Override
    public JQuickAvgAggregator<Number> getInitialValue() {
        return new JQuickAvgAggregator<>();
    }



    @Override
    public JQuickAvgAggregator<Number> apply(JQuickRow row) {
        Number value = row.getAs(sourceColumn, Number.class);
        JQuickAvgAggregator<Number> aggregator = new JQuickAvgAggregator<>();
        aggregator.add(value);
        return aggregator;
    }

    @Override
    public JQuickAvgAggregator<Number> accumulate(JQuickAvgAggregator<Number> current, JQuickAvgAggregator<Number> next) {
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
    public JQuickAvgAggregator<Number> merge(JQuickAvgAggregator<Number> a, JQuickAvgAggregator<Number> b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的平均值
     *
     * @param aggregator 聚合累加器
     * @return 平均值，无数据时返回 null
     */
    public Double extractResult(JQuickAvgAggregator<Number> aggregator) {
        return aggregator == null ? null : aggregator.getAvgOrNull();
    }

    /**
     * 便捷方法：直接计算数据集某一列的平均值
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @return 平均值，无数据时返回 null
     */
    public static Double calculate(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.getRows().isEmpty()) {
            return null;
        }
        JQuickAvgProvider provider = new JQuickAvgProvider(sourceColumn, "avg");
        JQuickAvgAggregator<Number> result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickAvgAggregator<Number> value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }

    /**
     * 检查源列是否存在
     *
     * @param dataSet 数据集
     * @return true-存在，false-不存在
     */
    public boolean isSourceColumnExists(JQuickDataSet dataSet) {
        return dataSet.getColumns().stream().anyMatch(col -> col.getName().equals(sourceColumn));
    }

    /**
     * 获取源列的类型
     *
     * @param dataSet 数据集
     * @return 源列的类型，不存在时返回 null
     */
    public Class<?> getSourceColumnType(JQuickDataSet dataSet) {
        return dataSet.getColumns().stream()
                .filter(col -> col.getName().equals(sourceColumn))
                .findFirst()
                .map(col -> col.getType())
                .orElse(null);
    }
}