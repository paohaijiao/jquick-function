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
package com.github.paohaijiao.core.constant;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * packageName com.github.paohaijiao.core.constant
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/5
 */
public class JQuickAggregateProviderMethodConstants {

    public static final String SUM = "sum";

    public static final String AVG = "avg";

    public static final String COUNT = "count";

    public static final String MAX = "max";

    public static final String MIN = "min";

    public static final String MEDIAN = "median";

    public static final String STDDEV = "stddev";

    public static final String VARIANCE = "variance";

    public static final String FIRST = "first";

    public static final String LAST = "last";

    private static final Map<String, Object> METHOD_MAP = new HashMap<>();

    static {
        putMethod(SUM, SUM);
        putMethod(AVG, AVG);
        putMethod(COUNT, COUNT);
        putMethod(MAX, MAX);
        putMethod(MIN, MIN);
        putMethod(MEDIAN, MEDIAN);
        putMethod(STDDEV, STDDEV);
        putMethod(VARIANCE,VARIANCE);
        putMethod(FIRST, FIRST);
        putMethod(LAST,LAST);
    }
    /**
     * 获取方法 Map（返回副本，保证原始数据安全）
     */
    public static Map<String, Object> getMethodMap() {
        return new HashMap<>(METHOD_MAP);
    }

    /**
     * 添加或更新方法常量
     * @param key 方法名
     * @param value 方法对应的值
     * @return 之前的值，如果不存在则返回 null
     */
    public static Object putMethod(String key, Object value) {
        return METHOD_MAP.put(key, value);
    }

    /**
     * 批量添加方法
     * @param methods 方法名和值的 Map
     */
    public static void putAllMethods(Map<String, Object> methods) {
        METHOD_MAP.putAll(methods);
    }

    /**
     * 获取方法对应的值
     * @param key 方法名
     * @return 方法对应的值，不存在时返回 null
     */
    public static Object getMethod(String key) {
        return METHOD_MAP.get(key);
    }

    /**
     * 获取方法对应的值（带默认值）
     * @param key 方法名
     * @param defaultValue 默认值
     * @return 方法对应的值，不存在时返回默认值
     */
    public static Object getMethodOrDefault(String key, Object defaultValue) {
        return METHOD_MAP.getOrDefault(key, defaultValue);
    }

    /**
     * 移除方法
     * @param key 方法名
     * @return 被移除的值
     */
    public static Object removeMethod(String key) {
        return METHOD_MAP.remove(key);
    }

    /**
     * 判断方法是否存在
     * @param key 方法名
     * @return 存在返回 true
     */
    public static boolean containsMethod(String key) {
        return METHOD_MAP.containsKey(key);
    }

    /**
     * 获取所有方法名
     * @return 方法名集合
     */
    public static Set<String> getAllMethodNames() {
        return METHOD_MAP.keySet();
    }

    /**
     * 获取所有方法名（数组形式）
     * @return 方法名数组
     */
    public static String[] getAllMethodNamesAsArray() {
        return METHOD_MAP.keySet().toArray(new String[0]);
    }

    /**
     * 获取方法数量
     */
    public static int size() {
        return METHOD_MAP.size();
    }

    /**
     * 清空所有方法
     */
    public static void clear() {
        METHOD_MAP.clear();
    }

    /**
     * 判断是否为空
     */
    public static boolean isEmpty() {
        return METHOD_MAP.isEmpty();
    }



}
