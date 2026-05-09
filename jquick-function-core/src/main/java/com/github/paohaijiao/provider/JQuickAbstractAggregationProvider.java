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
package com.github.paohaijiao.provider;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * 聚合提供者抽象基类
 * 子类需要实现聚合逻辑：初始值、累加、合并
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/8
 */
public abstract class JQuickAbstractAggregationProvider<T> implements JQuickFunctionProvider<JQuickRow, T> {

    protected final String sourceColumn;

    protected final String targetField;

    protected final Class<T> targetClass;

    public JQuickAbstractAggregationProvider(String sourceColumn, String targetField, Class<T> targetClass) {
        this.sourceColumn = sourceColumn;
        this.targetField = targetField;
        this.targetClass = targetClass;
    }

    @Override
    public String getTargetField() {
        return targetField;
    }

    @Override
    public Class<?> getTargetClass() {
        return targetClass;
    }

    /**
     * 获取聚合初始值
     * 每个分组第一次累加时使用
     *
     * @return 初始值
     */
    public abstract T getInitialValue();

    /**
     * 累加：将当前行的值聚合到现有结果上
     *
     * @param current 当前的聚合结果
     * @param next    当前行提取的值
     * @return 累加后的新结果
     */
    public abstract T accumulate(T current, T next);

    /**
     * 合并两个聚合结果（用于并行计算场景）
     * 默认实现使用 accumulate，子类可覆盖优化
     *
     * @param a 聚合结果 A
     * @param b 聚合结果 B
     * @return 合并后的结果
     */
    public T merge(T a, T b) {
        //从初始值开始累加 b,将 b 累加到 a 上，这里依赖子类实现
        T result = a;
        return accumulate(a, b);
    }

    /**
     * 判断是否为聚合函数
     * 默认返回 true，非聚合函数子类可覆盖返回 false
     *
     * @return true-聚合函数，false-普通转换函数
     */
    public boolean isAggregate() {
        return true;
    }
}
