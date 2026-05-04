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
package com.github.paohaijiao.context;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 快速函数上下文
 * 用于在函数调用链中传递上下文参数
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionContext {


    private static final ThreadLocal<JQuickFunctionContext> current = new ThreadLocal<>();

    private final Map<Class<?>, Object> params = new ConcurrentHashMap<>();

    private final JQuickFunctionContext parent;

    /**
     * 构造函数 - 创建独立上下文
     */
    public JQuickFunctionContext() {
        this.parent = null;
    }


    /**
     * 构造函数 - 创建带父级上下文的实例
     *
     * @param parent 父级上下文
     */
    public JQuickFunctionContext(JQuickFunctionContext parent) {
        this.parent = parent;
    }

    /**
     * 获取当前线程的上下文
     *
     * @return 当前线程的上下文，可能为null
     */
    public static JQuickFunctionContext getCurrent() {
        return current.get();
    }

    /**
     * 设置当前线程的上下文
     *
     * @param context 上下文实例
     */
    public static void setCurrent(JQuickFunctionContext context) {
        current.set(context);
    }

    /**
     * 清除当前线程的上下文
     */
    public static void clearCurrent() {
        current.remove();
    }

    /**
     * 在当前线程中执行带上下文的任务
     *
     * @param context  上下文
     * @param runnable 要执行的任务
     */
    public static void runWithContext(JQuickFunctionContext context, Runnable runnable) {
        JQuickFunctionContext previous = getCurrent();
        try {
            setCurrent(context);
            runnable.run();
        } finally {
            setCurrent(previous);
        }
    }

    /**
     * 存入参数
     *
     * @param key   参数类型键
     * @param value 参数值
     * @param <T>   参数类型
     * @return 当前实例，支持链式调用
     */
    public <T> JQuickFunctionContext put(Class<T> key, T value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        params.put(key, value);
        return this;
    }

    /**
     * 批量存入参数
     *
     * @param map 参数映射表
     * @return 当前实例，支持链式调用
     */
    public JQuickFunctionContext putAll(Map<Class<?>, Object> map) {
        if (map != null) {
            params.putAll(map);
        }
        return this;
    }

    /**
     * 仅当不存在时才存入
     *
     * @param key   参数类型键
     * @param value 参数值
     * @param <T>   参数类型
     * @return 当前实例，支持链式调用
     */
    public <T> JQuickFunctionContext putIfAbsent(Class<T> key, T value) {
        params.putIfAbsent(key, value);
        return this;
    }

    /**
     * 获取参数
     * @param key 参数类型键
     * @param <T> 参数类型
     * @return 参数值，可能为null
     */
    public <T> T get(Class<T> key) {
        return get(key, null);
    }

    /**
     * 获取参数，支持默认值
     *
     * @param key          参数类型键
     * @param defaultValue 默认值
     * @param <T>          参数类型
     * @return 参数值或默认值
     */
    public <T> T get(Class<T> key, T defaultValue) {
        T value = (T) params.get(key);
        if (value == null && parent != null && value == null) {
            value = parent.get(key, null);
        }
        return value != null ? value : defaultValue;
    }

    /**
     * 获取参数，如果不存在则抛出异常
     *
     * @param key 参数类型键
     * @param <T> 参数类型
     * @return 参数值
     * @throws IllegalArgumentException 如果参数不存在
     */
    public <T> T getRequired(Class<T> key) {
        T value = get(key);
        if (value == null) {
            throw new IllegalArgumentException("Required parameter not found for type: " + key.getName());
        }
        return value;
    }

    /**
     * 检查是否包含指定类型的参数
     *
     * @param key 参数类型键
     * @return true-存在, false-不存在
     */
    public boolean contains(Class<?> key) {
        return params.containsKey(key) || (parent != null && parent.contains(key));
    }

    /**
     * 检查当前上下文是否包含（不检查父级）
     *
     * @param key 参数类型键
     * @return true-存在, false-不存在
     */
    public boolean containsLocal(Class<?> key) {
        return params.containsKey(key);
    }

    /**
     * 移除指定类型的参数
     *
     * @param key 参数类型键
     * @param <T> 参数类型
     * @return 被移除的参数值
     */
    public <T> T remove(Class<T> key) {
        return (T) params.remove(key);
    }

    /**
     * 清空所有参数
     */
    public void clear() {
        params.clear();
    }

    /**
     * 获取所有参数键集合
     *
     * @return 键集合
     */
    public Set<Class<?>> keySet() {
        return params.keySet();
    }

    /**
     * 获取所有参数值集合
     *
     * @return 值集合
     */
    public Collection<Object> values() {
        return params.values();
    }

    /**
     * 获取所有参数条目
     *
     * @return 条目集合
     */
    public Set<Map.Entry<Class<?>, Object>> entrySet() {
        return params.entrySet();
    }

    /**
     * 获取参数数量
     *
     * @return 参数数量
     */
    public int size() {
        return params.size();
    }

    /**
     * 检查上下文是否为空
     *
     * @return true-空, false-非空
     */
    public boolean isEmpty() {
        return params.isEmpty();
    }

    /**
     * 复制当前上下文（浅拷贝）
     *
     * @return 新的上下文实例
     */
    public JQuickFunctionContext copy() {
        JQuickFunctionContext copy = new JQuickFunctionContext(parent);
        copy.putAll(params);
        return copy;
    }

    /**
     * 创建独立副本（不保留父级引用）
     *
     * @return 独立的上下文实例
     */
    public JQuickFunctionContext copyIndependent() {
        JQuickFunctionContext copy = new JQuickFunctionContext();
        copy.putAll(params);
        return copy;
    }

    /**
     * 合并另一个上下文（后者覆盖前者）
     *
     * @param other 另一个上下文
     * @return 当前实例，支持链式调用
     */
    public JQuickFunctionContext merge(JQuickFunctionContext other) {
        if (other != null) {
            params.putAll(other.params);
        }
        return this;
    }

    /**
     * 如果存在则执行消费操作
     *
     * @param key      参数类型键
     * @param consumer 消费函数
     * @param <T>      参数类型
     * @return 当前实例，支持链式调用
     */
    public <T> JQuickFunctionContext ifPresent(Class<T> key, java.util.function.Consumer<T> consumer) {
        T value = get(key);
        if (value != null) {
            consumer.accept(value);
        }
        return this;
    }

    /**
     * 计算并更新值（如果值不存在则计算）
     *
     * @param key             参数类型键
     * @param mappingFunction 值计算函数
     * @param <T>             参数类型
     * @return 计算后的值
     */
    public <T> T computeIfAbsent(Class<T> key, java.util.function.Function<Class<T>, T> mappingFunction) {
        return (T) params.computeIfAbsent(key, k -> mappingFunction.apply(key));
    }

    @Override
    public String toString() {
        return "JQuickFunctionContext{" +
                "params=" + params +
                (parent != null ? ", parent=" + parent : "") +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickFunctionContext that = (JQuickFunctionContext) o;
        return params.equals(that.params);
    }

    @Override
    public int hashCode() {
        return params.hashCode();
    }
}
