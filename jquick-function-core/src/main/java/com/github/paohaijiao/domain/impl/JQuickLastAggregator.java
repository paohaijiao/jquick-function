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
 * 最后一个值累加器
 * 用于获取分组中的最后一个非空值
 *
 * @param <T> 值类型
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/9
 */
public class JQuickLastAggregator<T> implements Serializable, JQuickAggregator {

    private static final long serialVersionUID = 1L;

    private T value;
    private boolean hasValue = false;

    public JQuickLastAggregator() {
    }

    public JQuickLastAggregator(T value) {
        if (value != null) {
            this.value = value;
            this.hasValue = true;
        }
    }

    public void add(T newValue) {
        if (newValue != null) {
            this.value = newValue;
            this.hasValue = true;
        }
    }

    public void merge(JQuickLastAggregator<T> other) {
        if (other == null) {
            return;
        }
        if (other.hasValue) {
            this.value = other.value;
            this.hasValue = true;
        }
    }

    public T getLast() {
        return value;
    }

    public boolean hasValue() {
        return hasValue;
    }

    public boolean isEmpty() {
        return !hasValue;
    }

    public void reset() {
        value = null;
        hasValue = false;
    }

    @Override
    public String toString() {
        return String.format("LastAggregator{value=%s}", value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickLastAggregator<?> that = (JQuickLastAggregator<?>) o;
        return hasValue == that.hasValue && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, hasValue);
    }

    @Override
    public Object getResult() {
        return value;
    }
}
