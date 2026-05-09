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
 * 第一个值累加器
 * 用于获取分组中的第一个非空值
 *
 * @param <T> 值类型
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickFirstAggregator<T> implements Serializable, JQuickAggregator {

    private static final long serialVersionUID = 1L;

    private T value;
    private boolean hasValue = false;
    private boolean isFirstSet = false;

    public JQuickFirstAggregator() {
    }

    public JQuickFirstAggregator(T value) {
        if (value != null) {
            this.value = value;
            this.hasValue = true;
            this.isFirstSet = true;
        }
    }

    /**
     * 添加数值（只保留第一个非空值）
     *
     * @param newValue 数值（可为 null）
     */
    public void add(T newValue) {
        if (!isFirstSet && newValue != null) {
            this.value = newValue;
            this.hasValue = true;
            this.isFirstSet = true;
        }
    }

    /**
     * 合并另一个第一个值累加器
     * 优先保留当前已有的值，如果当前没有值则使用 other 的值
     *
     * @param other 另一个累加器
     */
    public void merge(JQuickFirstAggregator<T> other) {
        if (other == null) {
            return;
        }
        if (!isFirstSet && other.isFirstSet) {
            this.value = other.value;
            this.hasValue = other.hasValue;
            this.isFirstSet = other.isFirstSet;
        }
    }

    /**
     * 获取第一个值
     *
     * @return 第一个值，无数据时返回 null
     */
    public T getFirst() {
        return value;
    }

    /**
     * 判断是否已有第一个值
     *
     * @return true-已有，false-未有
     */
    public boolean isFirstSet() {
        return isFirstSet;
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
        value = null;
        hasValue = false;
        isFirstSet = false;
    }

    @Override
    public String toString() {
        return String.format("FirstAggregator{value=%s}", value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickFirstAggregator<?> that = (JQuickFirstAggregator<?>) o;
        return hasValue == that.hasValue &&
                isFirstSet == that.isFirstSet &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, hasValue, isFirstSet);
    }

    @Override
    public Object getResult() {
        return value;
    }
}
