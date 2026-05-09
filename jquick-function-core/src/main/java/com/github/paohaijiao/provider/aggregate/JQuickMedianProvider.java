package com.github.paohaijiao.provider.aggregate;
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
import com.github.paohaijiao.domain.impl.JQuickMedianAggregator;
import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * MEDIAN 聚合提供者 - 计算中位数
 *
 * 使用 MedianAggregator 作为内部累加器
 *
 * 使用示例：
 * <pre>
 * // 创建 MedianProvider
 * MedianProvider medianProvider = new MedianProvider("salary", "medianSalary");
 *
 * // 获取初始累加器
 * MedianAggregator<Number> aggregator = medianProvider.getInitialValue();
 *
 * // 累加每一行的值
 * for (JQuickRow row : dataSet.getRows()) {
 *     MedianAggregator<Number> value = medianProvider.apply(row);
 *     aggregator = medianProvider.accumulate(aggregator, value);
 * }
 *
 * // 获取最终的中位数
 * Double median = medianProvider.extractResult(aggregator);
 * </pre>
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class JQuickMedianProvider extends JQuickAbstractAggregationProvider<JQuickMedianAggregator> {

    /**
     * 构造函数
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     */
    public JQuickMedianProvider(String sourceColumn, String targetField) {
        super(sourceColumn, targetField, (Class) JQuickMedianAggregator.class);
    }

    @Override
    public JQuickMedianAggregator getInitialValue() {
        return new JQuickMedianAggregator<>();
    }

    @Override
    public String getName() {
        return JQuickAggregateProviderMethodConstants.MEDIAN;
    }

    @Override
    public JQuickMedianAggregator apply(JQuickRow row) {
        Number value = row.getAs(sourceColumn, Number.class);
        JQuickMedianAggregator aggregator = new JQuickMedianAggregator<>();
        aggregator.add(value);
        return aggregator;
    }

    @Override
    public JQuickMedianAggregator accumulate(JQuickMedianAggregator current, JQuickMedianAggregator next) {
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
    public JQuickMedianAggregator merge(JQuickMedianAggregator a, JQuickMedianAggregator b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的中位数
     *
     * @param aggregator 聚合累加器
     * @return 中位数，无数据时返回 null
     */
    public Double extractResult(JQuickMedianAggregator aggregator) {
        return aggregator == null ? null : aggregator.getMedian();
    }

    /**
     * 提取下四分位数
     *
     * @param aggregator 聚合累加器
     * @return 下四分位数
     */
    public Double extractFirstQuartile(JQuickMedianAggregator aggregator) {
        return aggregator == null ? null : aggregator.getFirstQuartile();
    }

    /**
     * 提取上四分位数
     *
     * @param aggregator 聚合累加器
     * @return 上四分位数
     */
    public Double extractThirdQuartile(JQuickMedianAggregator aggregator) {
        return aggregator == null ? null : aggregator.getThirdQuartile();
    }

    /**
     * 获取数据量
     *
     * @param aggregator 聚合累加器
     * @return 数据量
     */
    public Integer extractSize(JQuickMedianAggregator aggregator) {
        return aggregator == null ? 0 : aggregator.size();
    }

    /**
     * 便捷方法：直接计算数据集某一列的中位数
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @return 中位数，无数据时返回 null
     */
    public static Double calculate(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.getRows().isEmpty()) {
            return null;
        }
        JQuickMedianProvider provider = new JQuickMedianProvider(sourceColumn, "median");
        JQuickMedianAggregator result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickMedianAggregator value = provider.apply(row);
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
