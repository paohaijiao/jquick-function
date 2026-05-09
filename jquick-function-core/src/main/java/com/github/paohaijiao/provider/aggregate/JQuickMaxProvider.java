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

import com.github.paohaijiao.domain.impl.JQuickMaxAggregator;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;

/**
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class JQuickMaxProvider extends JQuickAbstractAggregationProvider<JQuickMaxAggregator> {

    /**
     * 构造函数
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     */
    public JQuickMaxProvider(String sourceColumn, String targetField) {
        super(sourceColumn, targetField, (Class) JQuickMaxAggregator.class);
    }

    @Override
    public JQuickMaxAggregator getInitialValue() {
        return new JQuickMaxAggregator<>();
    }

    @Override
    public JQuickMaxAggregator apply(JQuickRow row) {
        Number value = row.getAs(sourceColumn, Number.class);
        JQuickMaxAggregator aggregator = new JQuickMaxAggregator<>();
        aggregator.add(value);
        return aggregator;
    }

    @Override
    public JQuickMaxAggregator accumulate(JQuickMaxAggregator current, JQuickMaxAggregator next) {
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
    public JQuickMaxAggregator merge(JQuickMaxAggregator a, JQuickMaxAggregator b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的最大值
     *
     * @param aggregator 聚合累加器
     * @return 最大值，无数据时返回 null
     */
    public Double extractResult(JQuickMaxAggregator aggregator) {
        return aggregator == null ? null : aggregator.getMaxAsDouble();
    }

    /**
     * 从聚合结果中提取最终的最大值（保持原类型）
     *
     * @param aggregator 聚合累加器
     * @return 最大值，无数据时返回 null
     */
    public Number extractResultAsNumber(JQuickMaxAggregator aggregator) {
        return aggregator == null ? null : aggregator.getMax();
    }

    /**
     * 便捷方法：直接计算数据集某一列的最大值
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @return 最大值，无数据时返回 null
     */
    public static Double calculate(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.isEmpty()) {
            return null;
        }
        JQuickMaxProvider provider = new JQuickMaxProvider(sourceColumn, "max");
        JQuickMaxAggregator result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickMaxAggregator value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }
}
