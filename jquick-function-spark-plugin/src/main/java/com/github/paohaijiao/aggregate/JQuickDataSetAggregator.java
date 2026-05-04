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
package com.github.paohaijiao.aggregate;

/**
 * packageName com.github.paohaijiao.aggregate
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */

import com.github.paohaijiao.manage.JQuickFunctionManager;
import com.github.paohaijiao.statement.JQuickDataSet;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 数据集聚合构建器
 */
public class JQuickDataSetAggregator {

    private final JQuickDataSet dataset;

    private final List<String> groupByColumns = new ArrayList<>();

    private Map<String, List<Object>> aggregations;

    private String resultName = "aggregated_result";

    private JQuickDataSetAggregator(JQuickDataSet dataset) {
        this.dataset = dataset;
    }

    /**
     * 开始聚合构建
     */
    public static JQuickDataSetAggregator on(JQuickDataSet dataset) {
        return new JQuickDataSetAggregator(dataset);
    }

    /**
     * 设置分组列
     */
    public JQuickDataSetAggregator groupBy(String... columns) {
        this.groupByColumns.addAll(Arrays.asList(columns));
        return this;
    }

    /**
     * 设置分组列
     */
    public JQuickDataSetAggregator groupBy(List<String> columns) {
        this.groupByColumns.addAll(columns);
        return this;
    }

    /**
     * 设置聚合配置
     *
     * @param aggregations Map格式：{"functionName": ["col1", "col2", ...]}
     */
    public JQuickDataSetAggregator aggregations(Map<String, List<Object>> aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    /**
     * 便捷方法：添加求和聚合
     */
    public JQuickDataSetAggregator sum(String... columns) {
        return addAggregation("sum", Arrays.asList(columns));
    }

    /**
     * 便捷方法：添加平均值聚合
     */
    public JQuickDataSetAggregator avg(String... columns) {
        return addAggregation("avg", Arrays.asList(columns));
    }

    /**
     * 便捷方法：添加计数聚合
     */
    public JQuickDataSetAggregator count(String... columns) {
        return addAggregation("count", Arrays.asList(columns));
    }

    /**
     * 便捷方法：添加去重计数聚合
     */
    public JQuickDataSetAggregator countDistinct(String... columns) {
        return addAggregation("count_distinct", Arrays.asList(columns));
    }

    /**
     * 便捷方法：添加最大值聚合
     */
    public JQuickDataSetAggregator max(String... columns) {
        return addAggregation("max", Arrays.asList(columns));
    }

    /**
     * 便捷方法：添加最小值聚合
     */
    public JQuickDataSetAggregator min(String... columns) {
        return addAggregation("min", Arrays.asList(columns));
    }

    /**
     * 添加聚合
     */
    private JQuickDataSetAggregator addAggregation(String functionName, List<String> columns) {
        if (this.aggregations == null) {
            this.aggregations = new LinkedHashMap<>();
        }
        List<Object> objColumns = new ArrayList<>(columns);
        this.aggregations.put(functionName, objColumns);
        return this;
    }

    /**
     * 设置结果数据集名称
     */
    public JQuickDataSetAggregator as(String resultName) {
        this.resultName = resultName;
        return this;
    }

    /**
     * 执行聚合
     */
    public JQuickDataSet execute() {
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalStateException("At least one aggregation must be specified");
        }
        JQuickDataSetAggregateFunction function = new JQuickDataSetAggregateFunction()
                .withGroupBy(groupByColumns.isEmpty() ? null : new ArrayList<>(groupByColumns))
                .withAggregations(new LinkedHashMap<>(aggregations))
                .withResultName(resultName);
        return JQuickFunctionManager.dispatch(function, dataset);
    }

    /**
     * 异步执行聚合
     */
    public CompletableFuture<JQuickDataSet> executeAsync() {
        if (aggregations == null || aggregations.isEmpty()) {
            throw new IllegalStateException("At least one aggregation must be specified");
        }
        JQuickDataSetAggregateFunction function = new JQuickDataSetAggregateFunction()
                .withGroupBy(groupByColumns.isEmpty() ? null : new ArrayList<>(groupByColumns))
                .withAggregations(new LinkedHashMap<>(aggregations))
                .withResultName(resultName);
        return JQuickFunctionManager.dispatchAsync(function, dataset);
    }
}
