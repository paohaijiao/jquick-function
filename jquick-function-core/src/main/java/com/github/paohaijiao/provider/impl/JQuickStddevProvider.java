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
package com.github.paohaijiao.provider.impl;

import com.github.paohaijiao.domain.JQuickStddevAggregator;
import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * STDDEV 聚合提供者 - 计算标准差
 *
 * 支持两种模式：
 * - POPULATION：总体标准差（除以 n）
 * - SAMPLE：样本标准差（除以 n-1）
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class JQuickStddevProvider extends JQuickAbstractAggregationProvider<JQuickStddevAggregator> {

    /**
     * 标准差类型
     */
    public enum StddevType {
        /** 总体标准差 - 除以 n */
        POPULATION,
        /** 样本标准差 - 除以 n-1 */
        SAMPLE
    }

    private final StddevType stddevType;

    /**
     * 构造函数 - 默认使用样本标准差
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     */
    public JQuickStddevProvider(String sourceColumn, String targetField) {
        this(sourceColumn, targetField, StddevType.SAMPLE);
    }

    /**
     * 构造函数 - 指定标准差类型
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     * @param stddevType   标准差类型
     */
    public JQuickStddevProvider(String sourceColumn, String targetField, StddevType stddevType) {
        super(sourceColumn, targetField, (Class) JQuickStddevAggregator.class);
        this.stddevType = stddevType;
    }

    @Override
    public JQuickStddevAggregator getInitialValue() {
        return new JQuickStddevAggregator();
    }

    @Override
    public JQuickStddevAggregator apply(JQuickRow row) {
        Number value = row.getAs(sourceColumn, Number.class);
        JQuickStddevAggregator aggregator = new JQuickStddevAggregator();
        aggregator.add(value);
        return aggregator;
    }

    @Override
    public JQuickStddevAggregator accumulate(JQuickStddevAggregator current, JQuickStddevAggregator next) {
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
    public JQuickStddevAggregator merge(JQuickStddevAggregator a, JQuickStddevAggregator b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的标准差
     *
     * @param aggregator 聚合累加器
     * @return 标准差，无数据时返回 null
     */
    public Double extractResult(JQuickStddevAggregator aggregator) {
        if (aggregator == null || aggregator.isEmpty()) {
            return null;
        }
        return stddevType == StddevType.POPULATION ? aggregator.getPopulationStddev() : aggregator.getSampleStddev();
    }

    /**
     * 获取总体标准差
     */
    public Double extractPopulationStddev(JQuickStddevAggregator aggregator) {
        return aggregator == null ? null : aggregator.getPopulationStddev();
    }

    /**
     * 获取样本标准差
     */
    public Double extractSampleStddev(JQuickStddevAggregator aggregator) {
        return aggregator == null ? null : aggregator.getSampleStddev();
    }

    /**
     * 便捷方法：直接计算数据集某一列的样本标准差
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @return 样本标准差，无数据时返回 null
     */
    public static Double calculateSample(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.getRows().size() < 2) {
            return null;
        }
        JQuickStddevProvider provider = new JQuickStddevProvider(sourceColumn, "stddev", StddevType.SAMPLE);
        JQuickStddevAggregator result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickStddevAggregator value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }

    /**
     * 便捷方法：直接计算数据集某一列的总体标准差
     */
    public static Double calculatePopulation(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.getRows().isEmpty()) {
            return null;
        }
        JQuickStddevProvider provider = new JQuickStddevProvider(sourceColumn, "stddev", StddevType.POPULATION);
        JQuickStddevAggregator result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickStddevAggregator value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }
}
