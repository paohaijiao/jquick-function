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
package com.github.paohaijiao.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flink聚合器（用于DataStream API）
 * 支持多种聚合函数的组合
 */
public class FlinkAggregator implements AggregateFunction<Tuple2<String, Row>, FlinkAggregator.Accumulator, Row> {

    private final Map<String, List<Object>> aggregations;

    private final List<String> groupByColumns;

    private final Map<String, Class<?>> columnTypes;

    public FlinkAggregator(Map<String, List<Object>> aggregations, List<String> groupByColumns) {
        this.aggregations = aggregations;
        this.groupByColumns = groupByColumns;
        this.columnTypes = new HashMap<>();
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(Tuple2<String, Row> value, Accumulator accumulator) {
        Row row = value.f1;
        accumulator.groupKey = value.f0; // 设置分组键
        if (groupByColumns != null) {// 提取分组列的值
            for (int i = 0; i < groupByColumns.size(); i++) {
                String colName = groupByColumns.get(i);
                Object colValue = row.getField(colName);
                accumulator.groupValues.put(colName, colValue);
            }
        }
        for (Map.Entry<String, List<Object>> entry : aggregations.entrySet()) { // 处理每个聚合函数
            String functionName = entry.getKey();
            List<Object> columns = entry.getValue();
            for (Object col : columns) {
                String columnName = col.toString();
                String aggKey = functionName + "_" + columnName;
                Object fieldValue = row.getField(columnName);
                if (fieldValue == null) continue;
                switch (functionName.toLowerCase()) {
                    case "sum":
                    case "avg":
                        accumulateSumAvg(accumulator, aggKey, fieldValue, functionName);
                        break;
                    case "count":
                        accumulateCount(accumulator, aggKey);
                        break;
                    case "count_distinct":
                        accumulateCountDistinct(accumulator, aggKey, fieldValue);
                        break;
                    case "max":
                        accumulateMax(accumulator, aggKey, fieldValue);
                        break;
                    case "min":
                        accumulateMin(accumulator, aggKey, fieldValue);
                        break;
                    case "collect_list":
                        accumulateCollectList(accumulator, aggKey, fieldValue);
                        break;
                    case "collect_set":
                        accumulateCollectSet(accumulator, aggKey, fieldValue);
                        break;
                }
            }
        }

        return accumulator;
    }

    /**
     * 累加求和/平均值
     */
    private void accumulateSumAvg(Accumulator accumulator, String aggKey, Object value, String functionName) {
        double numValue = toDouble(value);
        Double currentSum = (Double) accumulator.aggValues.getOrDefault(aggKey, 0.0);// 累加和
        accumulator.aggValues.put(aggKey, currentSum + numValue);
        Long currentCount = accumulator.counts.getOrDefault(aggKey, 0L);// 累加计数（用于平均值）
        accumulator.counts.put(aggKey, currentCount + 1);
    }

    /**
     * 累加计数
     */
    private void accumulateCount(Accumulator accumulator, String aggKey) {
        Long currentCount = (Long) accumulator.aggValues.getOrDefault(aggKey, 0L);
        accumulator.aggValues.put(aggKey, currentCount + 1);
    }

    /**
     * 累加去重计数
     */
    private void accumulateCountDistinct(Accumulator accumulator, String aggKey, Object value) {
        Set<Object> distinctSet = accumulator.distinctSets.getOrDefault(aggKey, new HashSet<>());
        distinctSet.add(value);
        accumulator.distinctSets.put(aggKey, distinctSet);
        accumulator.aggValues.put(aggKey, (long) distinctSet.size());
    }

    /**
     * 累加最大值
     */
    private void accumulateMax(Accumulator accumulator, String aggKey, Object value) {
        double numValue = toDouble(value);
        Double currentMax = (Double) accumulator.aggValues.getOrDefault(aggKey, Double.MIN_VALUE);
        accumulator.aggValues.put(aggKey, Math.max(currentMax, numValue));
    }

    /**
     * 累加最小值
     */
    private void accumulateMin(Accumulator accumulator, String aggKey, Object value) {
        double numValue = toDouble(value);
        Double currentMin = (Double) accumulator.aggValues.getOrDefault(aggKey, Double.MAX_VALUE);
        accumulator.aggValues.put(aggKey, Math.min(currentMin, numValue));
    }

    /**
     * 收集为列表
     */
    private void accumulateCollectList(Accumulator accumulator, String aggKey, Object value) {
        List<Object> list = accumulator.lists.getOrDefault(aggKey, new ArrayList<>());
        list.add(value);
        accumulator.lists.put(aggKey, list);
        accumulator.aggValues.put(aggKey, list);
    }

    /**
     * 收集为集合（去重）
     */
    private void accumulateCollectSet(Accumulator accumulator, String aggKey, Object value) {
        Set<Object> set = accumulator.sets.getOrDefault(aggKey, new HashSet<>());
        set.add(value);
        accumulator.sets.put(aggKey, set);
        accumulator.aggValues.put(aggKey, set);
    }

    @Override
    public Row getResult(Accumulator accumulator) {
        List<Object> resultValues = new ArrayList<>();// 计算最终结果
        if (groupByColumns != null) {// 添加分组列的值
            for (String colName : groupByColumns) {
                resultValues.add(accumulator.groupValues.get(colName));
            }
        }
        for (Map.Entry<String, List<Object>> entry : aggregations.entrySet()) { // 添加聚合结果
            String functionName = entry.getKey();
            List<Object> columns = entry.getValue();
            for (Object col : columns) {
                String columnName = col.toString();
                String aggKey = functionName + "_" + columnName;
                Object result = null;
                switch (functionName.toLowerCase()) {
                    case "avg":
                        Double sum = (Double) accumulator.aggValues.get(aggKey);
                        Long count = accumulator.counts.get(aggKey);
                        result = count != null && count > 0 ? sum / count : null;
                        break;
                    case "count_distinct":
                        result = accumulator.aggValues.get(aggKey);
                        break;
                    case "collect_list":
                        result = accumulator.aggValues.get(aggKey);
                        break;
                    case "collect_set":
                        result = accumulator.aggValues.get(aggKey);
                        break;
                    default:
                        result = accumulator.aggValues.get(aggKey);
                        break;
                }
                resultValues.add(result);
            }
        }
        return Row.of(resultValues.toArray()); // 构建Row对象
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        if (a.groupKey == null && b.groupKey != null) {// 合并分组值
            a.groupKey = b.groupKey;
        }
        a.groupValues.putAll(b.groupValues);// 合并分组列值
        for (Map.Entry<String, Object> entry : b.aggValues.entrySet()) {// 合并聚合值
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.startsWith("sum_") || key.startsWith("avg_")) {
                Double current = (Double) a.aggValues.getOrDefault(key, 0.0);
                a.aggValues.put(key, current + (Double) value);
            } else if (key.startsWith("max_")) {
                Double current = (Double) a.aggValues.getOrDefault(key, Double.MIN_VALUE);
                Double newValue = (Double) value;
                a.aggValues.put(key, Math.max(current, newValue));
            } else if (key.startsWith("min_")) {
                Double current = (Double) a.aggValues.getOrDefault(key, Double.MAX_VALUE);
                Double newValue = (Double) value;
                a.aggValues.put(key, Math.min(current, newValue));
            } else if (key.startsWith("count_") && !key.startsWith("count_distinct_")) {
                Long current = (Long) a.aggValues.getOrDefault(key, 0L);
                a.aggValues.put(key, current + (Long) value);
            } else {
                a.aggValues.put(key, value);
            }
        }
        // 合并计数
        for (Map.Entry<String, Long> entry : b.counts.entrySet()) {
            Long current = a.counts.getOrDefault(entry.getKey(), 0L);
            a.counts.put(entry.getKey(), current + entry.getValue());
        }
        // 合并去重集合
        for (Map.Entry<String, Set<Object>> entry : b.distinctSets.entrySet()) {
            Set<Object> current = a.distinctSets.getOrDefault(entry.getKey(), new HashSet<>());
            current.addAll(entry.getValue());
            a.distinctSets.put(entry.getKey(), current);
            a.aggValues.put(entry.getKey(), (long) current.size());
        }
        // 合并列表
        for (Map.Entry<String, List<Object>> entry : b.lists.entrySet()) {
            List<Object> current = a.lists.getOrDefault(entry.getKey(), new ArrayList<>());
            current.addAll(entry.getValue());
            a.lists.put(entry.getKey(), current);
            a.aggValues.put(entry.getKey(), current);
        }
        // 合并集合
        for (Map.Entry<String, Set<Object>> entry : b.sets.entrySet()) {
            Set<Object> current = a.sets.getOrDefault(entry.getKey(), new HashSet<>());
            current.addAll(entry.getValue());
            a.sets.put(entry.getKey(), current);
            a.aggValues.put(entry.getKey(), current);
        }
        return a;
    }

    /**
     * 转换为double
     */
    private double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return 0.0;
    }

    /**
     * 累加器类
     */
    public static class Accumulator {

        public String groupKey;// 存储分组键

        public Map<String, Object> groupValues = new LinkedHashMap<>();// 存储分组列的值

        public Map<String, Object> aggValues = new ConcurrentHashMap<>();//存储每个聚合函数的累加值 key: functionName_columnName, value: 累加器对象

        public Map<String, Long> counts = new ConcurrentHashMap<>();// 计数（用于平均值计算）

        public Map<String, Set<Object>> distinctSets = new ConcurrentHashMap<>(); // 去重集合（用于count_distinct）

        public Map<String, List<Object>> lists = new ConcurrentHashMap<>();// 列表收集（用于collect_list）

        public Map<String, Set<Object>> sets = new ConcurrentHashMap<>();// 集合收集（用于collect_set）
    }
}
