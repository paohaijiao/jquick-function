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
 * 计数累加器
 * 用于统计行数或非空值数量
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickCountAggregator implements Serializable, JQuickAggregator {

    private static final long serialVersionUID = 1L;

    private long count = 0;

    public JQuickCountAggregator() {
    }

    public JQuickCountAggregator(long count) {
        this.count = count;
    }

    /**
     * 计数 +1
     */
    public void increment() {
        count++;
    }

    /**
     * 增加指定数量
     *
     * @param n 增加的数量
     */
    public void add(long n) {
        count += n;
    }

    /**
     * 合并另一个累加器
     *
     * @param other 另一个累加器
     */
    public void merge(JQuickCountAggregator other) {
        if (other != null) {
            this.count += other.count;
        }
    }

    /**
     * 获取计数值
     *
     * @return 计数值
     */
    public long getCount() {
        return count;
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
    }

    @Override
    public String toString() {
        return String.format("CountAggregator{count=%d}", count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickCountAggregator that = (JQuickCountAggregator) o;
        return count == that.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count);
    }

    @Override
    public Object getResult() {
        return count;
    }
}
