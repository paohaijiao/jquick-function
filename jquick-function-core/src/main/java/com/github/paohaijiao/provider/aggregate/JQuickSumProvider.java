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
import com.github.paohaijiao.domain.impl.JQuickSumAggregator;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;

import java.math.BigDecimal;

/**
 * SUM 聚合提供者 - 计算总和
 * </pre>
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickSumProvider extends JQuickAbstractAggregationProvider<JQuickSumAggregator> {

    /**
     * 构造函数
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     */
    public JQuickSumProvider(String sourceColumn, String targetField) {
        super(sourceColumn, targetField, JQuickSumAggregator.class);
    }

    @Override
    public JQuickSumAggregator getInitialValue() {
        return new JQuickSumAggregator();
    }

    @Override
    public String getName() {
        return JQuickAggregateProviderMethodConstants.SUM;
    }

    @Override
    public JQuickSumAggregator apply(JQuickRow row) {
        Number value = row.getAs(sourceColumn, Number.class);
        return new JQuickSumAggregator(value);
    }

    @Override
    public JQuickSumAggregator accumulate(JQuickSumAggregator current, JQuickSumAggregator next) {
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
    public JQuickSumAggregator merge(JQuickSumAggregator a, JQuickSumAggregator b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的总和（double 类型）
     *
     * @param aggregator 聚合累加器
     * @return 总和，无数据时返回 null
     */
    public Double extractResult(JQuickSumAggregator aggregator) {
        return aggregator == null || aggregator.isEmpty() ? null : aggregator.getSumAsDouble();
    }

    /**
     * 从聚合结果中提取最终的总和（long 类型）
     *
     * @param aggregator 聚合累加器
     * @return 总和，无数据时返回 null
     * @throws ArithmeticException 如果值超出 long 范围
     */
    public Long extractResultAsLong(JQuickSumAggregator aggregator) {
        return aggregator == null || aggregator.isEmpty() ? null : aggregator.getSumAsLong();
    }

    /**
     * 从聚合结果中提取最终的总和（BigDecimal 类型）
     *
     * @param aggregator 聚合累加器
     * @return 总和，无数据时返回 null
     */
    public BigDecimal extractResultAsBigDecimal(JQuickSumAggregator aggregator) {
        return aggregator == null || aggregator.isEmpty() ? null : aggregator.getSumAsBigDecimal();
    }

    /**
     * 便捷方法：直接计算数据集某一列的总和
     *
     * @param dataSet      数据集
     * @param sourceColumn 源列名
     * @return 总和，无数据时返回 null
     */
    public static Double calculate(JQuickDataSet dataSet, String sourceColumn) {
        if (dataSet == null || dataSet.getRows().isEmpty()) {
            return null;
        }
        JQuickSumProvider provider = new JQuickSumProvider(sourceColumn, "sum");
        JQuickSumAggregator result = provider.getInitialValue();
        for (JQuickRow row : dataSet.getRows()) {
            JQuickSumAggregator value = provider.apply(row);
            result = provider.accumulate(result, value);
        }
        return provider.extractResult(result);
    }
}