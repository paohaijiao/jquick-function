/// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
// */
//package com.github.paohaijiao.domain;
//
///**
// * packageName com.github.paohaijiao.domain
// *
// * @author Martin
// * @version 1.0.0
// * @since 2026/5/4
// */
//
//import com.github.paohaijiao.aggregate.JQuickDataSetFlinkAggregateFunction;
//import com.github.paohaijiao.manage.JQuickFunctionManager;
//import com.github.paohaijiao.statement.JQuickDataSet;
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//
///**
// * Flink数据集聚合构建器
// */
//public class JQuickDataSetFlinkAggregator {
//
//    private final JQuickDataSet dataset;
//
//    private final List<String> groupByColumns = new ArrayList<>();
//
//    private final Map<String, List<Object>> aggregations = new LinkedHashMap<>();
//
//    private String resultName = "flink_aggregated_result";
//
//    private String executionMode = "batch"; // batch, streaming, table
//
//    private JQuickDataSetFlinkAggregator(JQuickDataSet dataset) {
//        this.dataset = dataset;
//    }
//
//    /**
//     * 开始聚合构建
//     */
//    public static JQuickDataSetFlinkAggregator on(JQuickDataSet dataset) {
//        return new JQuickDataSetFlinkAggregator(dataset);
//    }
//
//    /**
//     * 设置分组列
//     */
//    public JQuickDataSetFlinkAggregator groupBy(String... columns) {
//        this.groupByColumns.addAll(Arrays.asList(columns));
//        return this;
//    }
//
//    /**
//     * 设置分组列
//     */
//    public JQuickDataSetFlinkAggregator groupBy(List<String> columns) {
//        this.groupByColumns.addAll(columns);
//        return this;
//    }
//
//    /**
//     * 设置执行模式
//     *
//     * @param mode batch, streaming, table
//     */
//    public JQuickDataSetFlinkAggregator mode(String mode) {
//        this.executionMode = mode;
//        return this;
//    }
//
//    /**
//     * 求和聚合
//     */
//    public JQuickDataSetFlinkAggregator sum(String... columns) {
//        return addAggregation("sum", Arrays.asList(columns));
//    }
//
//    /**
//     * 平均值聚合
//     */
//    public JQuickDataSetFlinkAggregator avg(String... columns) {
//        return addAggregation("avg", Arrays.asList(columns));
//    }
//
//    /**
//     * 计数聚合
//     */
//    public JQuickDataSetFlinkAggregator count(String... columns) {
//        return addAggregation("count", Arrays.asList(columns));
//    }
//
//    /**
//     * 去重计数
//     */
//    public JQuickDataSetFlinkAggregator countDistinct(String... columns) {
//        return addAggregation("count_distinct", Arrays.asList(columns));
//    }
//
//    /**
//     * 最大值
//     */
//    public JQuickDataSetFlinkAggregator max(String... columns) {
//        return addAggregation("max", Arrays.asList(columns));
//    }
//
//    /**
//     * 最小值
//     */
//    public JQuickDataSetFlinkAggregator min(String... columns) {
//        return addAggregation("min", Arrays.asList(columns));
//    }
//
//    /**
//     * 标准差
//     */
//    public JQuickDataSetFlinkAggregator stddev(String... columns) {
//        return addAggregation("stddev", Arrays.asList(columns));
//    }
//
//    /**
//     * 方差
//     */
//    public JQuickDataSetFlinkAggregator variance(String... columns) {
//        return addAggregation("variance", Arrays.asList(columns));
//    }
//
//    /**
//     * 添加聚合
//     */
//    private JQuickDataSetFlinkAggregator addAggregation(String functionName, List<String> columns) {
//        List<Object> objColumns = new ArrayList<>(columns);
//        if (this.aggregations.containsKey(functionName)) {
//            this.aggregations.get(functionName).addAll(objColumns);
//        } else {
//            this.aggregations.put(functionName, objColumns);
//        }
//        return this;
//    }
//
//    /**
//     * 设置结果名称
//     */
//    public JQuickDataSetFlinkAggregator as(String resultName) {
//        this.resultName = resultName;
//        return this;
//    }
//
//    /**
//     * 设置聚合配置（直接使用Map）
//     */
//    public JQuickDataSetFlinkAggregator aggregations(Map<String, List<Object>> aggregations) {
//        this.aggregations.clear();
//        this.aggregations.putAll(aggregations);
//        return this;
//    }
//
//    /**
//     * 执行聚合
//     */
//    public JQuickDataSet execute() {
//        if (aggregations.isEmpty()) {
//            throw new IllegalStateException("At least one aggregation must be specified");
//        }
//        JQuickDataSetFlinkAgg regateFunction function = new JQuickDataSetFlinkAggregateFunction()
//                .withGroupBy(groupByColumns.isEmpty() ? null : new ArrayList<>(groupByColumns))
//                .withAggregations(new LinkedHashMap<>(aggregations))
//                .withResultName(resultName)
//                .withExecutionMode(executionMode);
//        return JQuickFunctionManager.dispatch(function, dataset);
//    }
//
//    /**
//     * 异步执行
//     */
//    public CompletableFuture<JQuickDataSet> executeAsync() {
//        if (aggregations.isEmpty()) {
//            throw new IllegalStateException("At least one aggregation must be specified");
//        }
//        JQuickDataSetFlinkAggregateFunction function = new JQuickDataSetFlinkAggregateFunction()
//                .withGroupBy(groupByColumns.isEmpty() ? null : new ArrayList<>(groupByColumns))
//                .withAggregations(new LinkedHashMap<>(aggregations))
//                .withResultName(resultName)
//                .withExecutionMode(executionMode);
//        return JQuickFunctionManager.dispatchAsync(function, dataset);
//    }
//}