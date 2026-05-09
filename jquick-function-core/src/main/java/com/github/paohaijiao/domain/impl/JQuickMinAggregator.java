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
package com.github.paohaijiao.domain.impl;

import com.github.paohaijiao.domain.JQuickAggregator;

import java.io.Serializable;
import java.util.Objects;

/**
 * 最小值累加器
 * 用于计算一组数值中的最小值
 *
 * @param <T> 数值类型
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickMinAggregator<T extends Number & Comparable<T>> implements Serializable, JQuickAggregator {

    private static final long serialVersionUID = 1L;

    private T minValue;

    private boolean hasValue = false;

    public JQuickMinAggregator() {
    }

    public JQuickMinAggregator(T value) {
        if (value != null) {
            this.minValue = value;
            this.hasValue = true;
        }
    }

    /**
     * 添加数值，更新最小值
     *
     * @param value 数值（可为 null）
     */
    public void add(T value) {
        if (value != null) {
            if (!hasValue || value.compareTo(minValue) < 0) {
                this.minValue = value;
                this.hasValue = true;
            }
        }
    }

    /**
     * 添加 double 值
     *
     * @param value double 值
     */
    @SuppressWarnings("unchecked")
    public void add(double value) {
        Double doubleValue = value;
        if (!hasValue || doubleValue.compareTo((Double) minValue) < 0) {
            this.minValue = (T) doubleValue;
            this.hasValue = true;
        }
    }

    /**
     * 添加 long 值
     *
     * @param value long 值
     */
    @SuppressWarnings("unchecked")
    public void add(long value) {
        Long longValue = value;
        if (!hasValue || longValue.compareTo((Long) minValue) < 0) {
            this.minValue = (T) longValue;
            this.hasValue = true;
        }
    }

    /**
     * 合并另一个最小值累加器
     *
     * @param other 另一个累加器
     */
    public void merge(JQuickMinAggregator<T> other) {
        if (other != null && other.hasValue) {
            add(other.minValue);
        }
    }

    /**
     * 获取最小值
     *
     * @return 最小值，无数据时返回 null
     */
    public T getMin() {
        return minValue;
    }

    /**
     * 获取最小值（double 类型）
     *
     * @return 最小值，无数据时返回 null
     */
    public Double getMinAsDouble() {
        return minValue != null ? minValue.doubleValue() : null;
    }

    /**
     * 获取最小值（long 类型）
     *
     * @return 最小值，无数据时返回 null
     */
    public Long getMinAsLong() {
        return minValue != null ? minValue.longValue() : null;
    }

    /**
     * 获取最小值（int 类型）
     *
     * @return 最小值，无数据时返回 null
     */
    public Integer getMinAsInt() {
        return minValue != null ? minValue.intValue() : null;
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
        minValue = null;
        hasValue = false;
    }

    @Override
    public String toString() {
        return String.format("MinAggregator{min=%s}", minValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickMinAggregator<?> that = (JQuickMinAggregator<?>) o;
        return hasValue == that.hasValue && Objects.equals(minValue, that.minValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minValue, hasValue);
    }

    @Override
    public Object getResult() {
        return minValue;
    }
}