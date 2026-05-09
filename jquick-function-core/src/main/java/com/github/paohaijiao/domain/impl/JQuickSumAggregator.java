package com.github.paohaijiao.domain.impl;
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

import com.github.paohaijiao.domain.JQuickAggregator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;

/**
 * 求和累加器
 * 存储总和，支持多种数值类型
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickSumAggregator implements Serializable, JQuickAggregator {

    private static final long serialVersionUID = 1L;

    private BigDecimal sum = BigDecimal.ZERO;

    private boolean hasValue = false;

    public JQuickSumAggregator() {
    }

    public JQuickSumAggregator(Number value) {
        if (value != null) {
            this.sum = new BigDecimal(value.toString());
            this.hasValue = true;
        }
    }

    /**
     * 添加数值
     *
     * @param value 数值（可为 null）
     */
    public void add(Number value) {
        if (value != null) {
            this.sum = this.sum.add(new BigDecimal(value.toString()));
            this.hasValue = true;
        }
    }

    /**
     * 添加 BigDecimal 值
     *
     * @param value BigDecimal 值
     */
    public void add(BigDecimal value) {
        if (value != null) {
            this.sum = this.sum.add(value);
            this.hasValue = true;
        }
    }

    /**
     * 添加 double 值
     *
     * @param value double 值
     */
    public void add(double value) {
        this.sum = this.sum.add(BigDecimal.valueOf(value));
        this.hasValue = true;
    }

    /**
     * 添加 long 值
     *
     * @param value long 值
     */
    public void add(long value) {
        this.sum = this.sum.add(BigDecimal.valueOf(value));
        this.hasValue = true;
    }

    /**
     * 合并另一个累加器
     *
     * @param other 另一个累加器
     */
    public void merge(JQuickSumAggregator other) {
        if (other != null && other.hasValue) {
            this.sum = this.sum.add(other.sum);
            this.hasValue = true;
        }
    }

    /**
     * 获取总和（double 类型）
     *
     * @return 总和
     */
    public double getSumAsDouble() {
        return sum.doubleValue();
    }

    /**
     * 获取总和（long 类型）
     *
     * @return 总和
     * @throws ArithmeticException 如果值超出 long 范围
     */
    public long getSumAsLong() {
        return sum.longValueExact();
    }

    /**
     * 获取总和（BigDecimal 类型）
     *
     * @return 总和
     */
    public BigDecimal getSumAsBigDecimal() {
        return sum;
    }

    /**
     * 获取总和（int 类型）
     *
     * @return 总和
     * @throws ArithmeticException 如果值超出 int 范围
     */
    public int getSumAsInt() {
        return sum.intValueExact();
    }

    /**
     * 判断是否有值
     *
     * @return true-有值，false-无值
     */
    public boolean hasValue() {
        return hasValue;
    }

    /**
     * 判断是否为空
     *
     * @return true-空，false-非空
     */
    public boolean isEmpty() {
        return !hasValue;
    }

    /**
     * 重置累加器
     */
    public void reset() {
        sum = BigDecimal.ZERO;
        hasValue = false;
    }

    @Override
    public String toString() {
        return String.format("SumAggregator{sum=%s}", sum.toPlainString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickSumAggregator that = (JQuickSumAggregator) o;
        return hasValue == that.hasValue && sum.compareTo(that.sum) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, hasValue);
    }

    @Override
    public Object getResult() {
        return sum.toPlainString();
    }
}
