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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 中位数累加器
 * 存储所有数值，用于计算中位数
 *
 * 注意：对于大数据集，此实现会存储所有数据，内存开销较大
 * 生产环境建议使用 TDigest 等近似算法
 *
 * @param <T> 数值类型
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickMedianAggregator<T extends Number & Comparable<T>> implements Serializable , JQuickAggregator {

    private static final long serialVersionUID = 1L;

    private final List<T> values = new ArrayList<>();

    private boolean sorted = false;

    public JQuickMedianAggregator() {
    }

    /**
     * 添加数值
     *
     * @param value 数值（可为 null）
     */
    public void add(T value) {
        if (value != null) {
            values.add(value);
            sorted = false;
        }
    }

    /**
     * 添加 double 值
     */
    @SuppressWarnings("unchecked")
    public void add(double value) {
        values.add((T) Double.valueOf(value));
        sorted = false;
    }

    /**
     * 添加 long 值
     */
    @SuppressWarnings("unchecked")
    public void add(long value) {
        values.add((T) Long.valueOf(value));
        sorted = false;
    }

    /**
     * 添加 int 值
     */
    @SuppressWarnings("unchecked")
    public void add(int value) {
        values.add((T) Integer.valueOf(value));
        sorted = false;
    }

    /**
     * 添加 float 值
     */
    @SuppressWarnings("unchecked")
    public void add(float value) {
        values.add((T) Float.valueOf(value));
        sorted = false;
    }

    /**
     * 批量添加数值
     *
     * @param values 数值列表
     */
    public void addAll(List<T> values) {
        if (values != null && !values.isEmpty()) {
            this.values.addAll(values);
            sorted = false;
        }
    }

    /**
     * 合并另一个中位数累加器
     *
     * @param other 另一个累加器
     */
    public void merge(JQuickMedianAggregator<T> other) {
        if (other != null && !other.values.isEmpty()) {
            this.values.addAll(other.values);
            sorted = false;
        }
    }

    /**
     * 排序（懒加载方式，仅在需要时排序）
     */
    private void ensureSorted() {
        if (!sorted && !values.isEmpty()) {
            Collections.sort(values);
            sorted = true;
        }
    }

    /**
     * 获取中位数
     * 数据量为奇数时返回中间值，偶数时返回中间两个数的平均值
     *
     * @return 中位数，无数据时返回 null
     */
    public Double getMedian() {
        if (values.isEmpty()) {
            return null;
        }
        ensureSorted();
        int size = values.size();
        if (size % 2 == 1) {
            // 奇数个，取中间值
            return values.get(size / 2).doubleValue();
        } else {
            // 偶数个，取中间两个的平均值
            double left = values.get(size / 2 - 1).doubleValue();
            double right = values.get(size / 2).doubleValue();
            return (left + right) / 2.0;
        }
    }

    /**
     * 获取下四分位数（Q1）
     *
     * @return 下四分位数，无数据时返回 null
     */
    public Double getFirstQuartile() {
        if (values.size() < 2) {
            return getMedian();
        }
        ensureSorted();
        int size = values.size();
        int quarter = size / 4;

        if (size % 4 == 0) {
            double left = values.get(quarter - 1).doubleValue();
            double right = values.get(quarter).doubleValue();
            return (left + right) / 2.0;
        } else {
            return values.get(quarter).doubleValue();
        }
    }

    /**
     * 获取上四分位数（Q3）
     *
     * @return 上四分位数，无数据时返回 null
     */
    public Double getThirdQuartile() {
        if (values.size() < 3) {
            return getMedian();
        }
        ensureSorted();
        int size = values.size();
        int threeQuarter = (size * 3) / 4;

        if ((size * 3) % 4 == 0) {
            double left = values.get(threeQuarter - 1).doubleValue();
            double right = values.get(threeQuarter).doubleValue();
            return (left + right) / 2.0;
        } else {
            return values.get(threeQuarter).doubleValue();
        }
    }

    /**
     * 获取所有值（只读）
     *
     * @return 值列表
     */
    public List<T> getValues() {
        return Collections.unmodifiableList(values);
    }

    /**
     * 获取数据量
     *
     * @return 数据量
     */
    public int size() {
        return values.size();
    }

    /**
     * 判断是否为空
     *
     * @return true-空，false-非空
     */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * 重置累加器
     */
    public void reset() {
        values.clear();
        sorted = false;
    }

    /**
     * 获取排序后的副本（用于调试）
     *
     * @return 排序后的列表副本
     */
    public List<T> getSortedValues() {
        ensureSorted();
        return new ArrayList<>(values);
    }

    @Override
    public String toString() {
        return String.format("MedianAggregator{size=%d, median=%s, q1=%s, q3=%s}", size(), getMedian(), getFirstQuartile(), getThirdQuartile());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickMedianAggregator<?> that = (JQuickMedianAggregator<?>) o;
        return sorted == that.sorted && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, sorted);
    }

    @Override
    public Object getResult() {
        return getMedian();
    }
}