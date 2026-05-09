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
 * 方差累加器
 * 使用 Welford 算法在线计算方差，避免存储所有数据
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickVarianceAggregator implements Serializable, JQuickAggregator {

    private static final long serialVersionUID = 1L;

    private long count = 0;          // 数据量

    private double mean = 0.0;       // 均值

    private double m2 = 0.0;         // 平方差累积和（用于计算方差）

    public JQuickVarianceAggregator() {
    }

    /**
     * 添加数值
     *
     * @param value 数值（可为 null）
     */
    public void add(Number value) {
        if (value != null) {
            add(value.doubleValue());
        }
    }

    /**
     * 添加 double 值
     *
     * @param value double 值
     */
    public void add(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        m2 += delta * delta2;
    }

    /**
     * 添加 long 值
     *
     * @param value long 值
     */
    public void add(long value) {
        add((double) value);
    }

    /**
     * 合并另一个方差累加器
     *
     * @param other 另一个累加器
     */
    public void merge(JQuickVarianceAggregator other) {
        if (other == null || other.count == 0) {
            return;
        }

        if (count == 0) {
            this.count = other.count;
            this.mean = other.mean;
            this.m2 = other.m2;
            return;
        }
        long n1 = this.count;
        long n2 = other.count;
        long n = n1 + n2;

        double mean1 = this.mean;
        double mean2 = other.mean;

        double newMean = (n1 * mean1 + n2 * mean2) / n;

        double delta = mean2 - mean1;
        double newM2 = this.m2 + other.m2 + delta * delta * n1 * n2 / n;

        this.count = n;
        this.mean = newMean;
        this.m2 = newM2;
    }

    /**
     * 获取数据量
     *
     * @return 数据量
     */
    public long getCount() {
        return count;
    }

    /**
     * 获取均值
     *
     * @return 均值
     */
    public double getMean() {
        return mean;
    }

    /**
     * 获取总体方差（除以 n）
     *
     * @return 总体方差，无数据时返回 null
     */
    public Double getPopulationVariance() {
        if (count == 0) {
            return null;
        }
        return m2 / count;
    }

    /**
     * 获取样本方差（除以 n-1）
     *
     * @return 样本方差，无数据时返回 null
     */
    public Double getSampleVariance() {
        if (count < 2) {
            return null;
        }
        return m2 / (count - 1);
    }

    /**
     * 判断是否为空
     *
     * @return true-空，false-非空
     */
    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * 重置累加器
     */
    public void reset() {
        count = 0;
        mean = 0.0;
        m2 = 0.0;
    }

    @Override
    public String toString() {
        return String.format("VarianceAggregator{count=%d, mean=%.4f, variance=%.4f}", count, mean, getPopulationVariance());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickVarianceAggregator that = (JQuickVarianceAggregator) o;
        return count == that.count &&
                Double.compare(that.mean, mean) == 0 &&
                Double.compare(that.m2, m2) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, mean, m2);
    }

    @Override
    public Object getResult() {
        return  getPopulationVariance();
    }
}