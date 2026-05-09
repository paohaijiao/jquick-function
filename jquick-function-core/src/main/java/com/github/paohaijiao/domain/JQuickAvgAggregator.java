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
package com.github.paohaijiao.domain;
import java.io.Serializable;
import java.util.Objects;

/**
 * 平均值累加器
 * 存储总和与计数，用于计算平均值
 *
 * @param <T> 数值类型
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickAvgAggregator<T extends Number> implements Serializable {

    private static final long serialVersionUID = 1L;

    private double sum = 0.0;

    private long count = 0;

    public JQuickAvgAggregator() {
    }

    /**
     * 从数值中添加值
     *
     * @param value 数值（可为 null）
     */
    public void add(T value) {
        if (value != null) {
            sum += value.doubleValue();
            count++;
        }
    }

    /**
     * 添加原始 double 值
     *
     * @param value double 值
     */
    public void add(double value) {
        sum += value;
        count++;
    }

    /**
     * 合并另一个累加器
     *
     * @param other 另一个累加器
     */
    public void merge(JQuickAvgAggregator<T> other) {
        if (other != null) {
            this.sum += other.sum;
            this.count += other.count;
        }
    }

    /**
     * 获取平均值（默认值 0.0）
     *
     * @return 平均值，无数据时返回 0.0
     */
    public double getAvg() {
        return count == 0 ? 0.0 : sum / count;
    }

    /**
     * 获取平均值（可能为 null）
     *
     * @return 平均值，无数据时返回 null
     */
    public Double getAvgOrNull() {
        return count == 0 ? null : sum / count;
    }

    /**
     * 获取总和
     *
     * @return 总和
     */
    public double getSum() {
        return sum;
    }

    /**
     * 获取计数
     *
     * @return 计数
     */
    public long getCount() {
        return count;
    }

    /**
     * 判断是否为空（没有添加任何数据）
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
        sum = 0.0;
        count = 0;
    }

    @Override
    public String toString() {
        return String.format("AvgAggregator{sum=%.4f, count=%d, avg=%.4f}", sum, count, getAvg());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickAvgAggregator<?> that = (JQuickAvgAggregator<?>) o;
        return Double.compare(that.sum, sum) == 0 && count == that.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, count);
    }
}