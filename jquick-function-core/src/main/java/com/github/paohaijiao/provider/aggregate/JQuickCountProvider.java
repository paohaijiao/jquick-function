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
import com.github.paohaijiao.domain.impl.JQuickCountAggregator;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;

/**
 * COUNT 聚合提供者 - 使用 CountAggregator 累加器
 *
 * 使用示例：
 * <pre>
 * // 创建 CountProvider（计数所有行）
 * CountProvider countProvider = new CountProvider("employeeCount");
 *
 * // 创建 CountProvider（计数指定列的非空值）
 * CountProvider countProvider = new CountProvider("salary", "salaryCount");
 * </pre>
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickCountProvider extends JQuickAbstractAggregationProvider<JQuickCountAggregator> {

    /** 是否只计数非空值（当 sourceColumn 不为 null 时生效） */
    private final boolean countNonNullOnly;

    /**
     * 构造函数 - 计数所有行（不依赖具体列）
     *
     * @param targetField 目标字段名
     */
    public JQuickCountProvider(String targetField) {
        super(null, targetField, JQuickCountAggregator.class);
        this.countNonNullOnly = false;
    }

    /**
     * 构造函数 - 计数指定列的非空值
     *
     * @param sourceColumn 源列名
     * @param targetField  目标字段名
     */
    public JQuickCountProvider(String sourceColumn, String targetField) {
        super(sourceColumn, targetField, JQuickCountAggregator.class);
        this.countNonNullOnly = true;
    }

    @Override
    public JQuickCountAggregator getInitialValue() {
        return new JQuickCountAggregator();
    }

    @Override
    public String getName() {
        return JQuickAggregateProviderMethodConstants.COUNT;
    }

    @Override
    public JQuickCountAggregator apply(JQuickRow row) {
        JQuickCountAggregator aggregator = new JQuickCountAggregator();
        if (sourceColumn == null) {            // 计数行数：每行计数 1
            aggregator.increment();
        } else if (countNonNullOnly) {
            // 只计数非空值
            Object value = row.get(sourceColumn);
            if (value != null) {
                aggregator.increment();
            }
        } else {
            // 计数所有值（包括 null）
            aggregator.increment();
        }

        return aggregator;
    }

    @Override
    public JQuickCountAggregator accumulate(JQuickCountAggregator current, JQuickCountAggregator next) {
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
    public JQuickCountAggregator merge(JQuickCountAggregator a, JQuickCountAggregator b) {
        if (a == null) return b;
        if (b == null) return a;
        a.merge(b);
        return a;
    }

    /**
     * 从聚合结果中提取最终的计数值
     *
     * @param aggregator 聚合累加器
     * @return 计数值，无数据时返回 0
     */
    public Long extractResult(JQuickCountAggregator aggregator) {
        return aggregator == null ? 0L : aggregator.getCount();
    }
}