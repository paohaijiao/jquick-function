
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
package com.github.paohaijiao.provider.aggregate;

import com.github.paohaijiao.core.constant.JQuickAggregateProviderMethodConstants;
import com.github.paohaijiao.domain.impl.JQuickVarianceAggregator;
import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * VARIANCE 聚合提供者 - 计算方差
 *
 * 支持两种模式：
 * - POPULATION：总体方差（除以 n）
 * - SAMPLE：样本方差（除以 n-1）
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class JQuickVarianceProvider extends JQuickAbstractAggregationProvider<JQuickVarianceAggregator> {

    /**
     * 方差类型
     */
    public enum VarianceType {
        /** 总体方差 - 除以 n */
        POPULATION,
        /** 样本方差 - 除以 n-1 */
        SAMPLE
    }

    private final VarianceType varianceType;

    /**
     * 构造函数 - 默认使用样本方差
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     */
    public JQuickVarianceProvider(String sourceColumn, String targetField) {
        this(sourceColumn, targetField, VarianceType.SAMPLE);
    }

    /**
     * 构造函数 - 指定方差类型
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     * @param varianceType 方差类型
     */
    public JQuickVarianceProvider(String sourceColumn, String targetField, VarianceType varianceType) {
        super(sourceColumn, targetField, (Class) JQuickVarianceAggregator.class);
        this.varianceType = varianceType;
    }

    @Override
    public JQuickVarianceAggregator getInitialValue() {
        return new JQuickVarianceAggregator();
    }

    @Override
    public String getName() {
        return JQuickAggregateProviderMethodConstants.VARIANCE;
    }

    @Override
    public JQuickVarianceAggregator apply(JQuickRow row) {
        Number value = row.getAs(sourceColumn, Number.class);
        JQuickVarianceAggregator aggregator = new JQuickVarianceAggregator();
        aggregator.add(value);
        return aggregator;
    }

    @Override
    public JQuickVarianceAggregator accumulate(JQuickVarianceAggregator current, JQuickVarianceAggregator next) {
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
    public JQuickVarianceAggregator merge(JQuickVarianceAggregator a, JQuickVarianceAggregator b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的方差
     *
     * @param aggregator 聚合累加器
     * @return 方差，无数据时返回 null
     */
    public Double extractResult(JQuickVarianceAggregator aggregator) {
        if (aggregator == null || aggregator.isEmpty()) {
            return null;
        }
        return varianceType == VarianceType.POPULATION ? aggregator.getPopulationVariance() : aggregator.getSampleVariance();
    }

    /**
     * 获取总体方差
     */
    public Double extractPopulationVariance(JQuickVarianceAggregator aggregator) {
        return aggregator == null ? null : aggregator.getPopulationVariance();
    }

    /**
     * 获取样本方差
     */
    public Double extractSampleVariance(JQuickVarianceAggregator aggregator) {
        return aggregator == null ? null : aggregator.getSampleVariance();
    }

    /**
     * 便捷方法：直接计算数据集某一列的样本方差
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @return 样本方差，无数据时返回 null
     */
    public static Double calculateSample(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.getRows().size() < 2) {
            return null;
        }
        JQuickVarianceProvider provider = new JQuickVarianceProvider(sourceColumn, "variance", VarianceType.SAMPLE);
        JQuickVarianceAggregator result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickVarianceAggregator value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }

    /**
     * 便捷方法：直接计算数据集某一列的总体方差
     */
    public static Double calculatePopulation(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.getRows().isEmpty()) {
            return null;
        }
        JQuickVarianceProvider provider = new JQuickVarianceProvider(sourceColumn, "variance", VarianceType.POPULATION);
        JQuickVarianceAggregator result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickVarianceAggregator value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }
}