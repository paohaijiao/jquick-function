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
package com.github.paohaijiao.provider.aggregate.impl;

/**
 * packageName com.github.paohaijiao.provider.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/5
 */
import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickFlinkComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickAggregateProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickFlinkGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Flink 分布式中位数聚合器
 */
public class JQuickFlinkMedianGroupByProvider extends JQuickFlinkGroupByAggregationProvider<Double> {

    private final String medianColumn;

    public JQuickFlinkMedianGroupByProvider(List<String> groupByColumns, String resultColumnName, String medianColumn, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        super(groupByColumns, resultColumnName, env, tableEnv);
        this.medianColumn = medianColumn;
    }

    @Override
    protected DataSet<JQuickRow> doAggregate(DataSet<JQuickRow> dataSet) throws Exception {
        // 转换为 Tuple2<分组键, 数值>
        DataSet<Tuple2<String, Double>> mapped = dataSet.map(new MapFunction<JQuickRow, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(JQuickRow row) throws Exception {
                String key = createGroupKey(row);
                Number value = row.getAs(medianColumn, Number.class);
                double num = value != null ? value.doubleValue() : 0.0;
                return new Tuple2<>(key, num);
            }
        });

        // 使用 GroupReduce 计算中位数
        DataSet<Tuple2<String, Double>> reduced = mapped
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, Double>> values,
                                       Collector<Tuple2<String, Double>> out) throws Exception {
                        List<Double> list = new ArrayList<>();
                        String key = null;

                        for (Tuple2<String, Double> val : values) {
                            key = val.f0;
                            list.add(val.f1);
                        }

                        Collections.sort(list);
                        double median;
                        int size = list.size();

                        if (size % 2 == 0) {
                            median = (list.get(size / 2 - 1) + list.get(size / 2)) / 2.0;
                        } else {
                            median = list.get(size / 2);
                        }

                        out.collect(new Tuple2<>(key, median));
                    }
                });

        return reduced.map(new MapFunction<Tuple2<String, Double>, JQuickRow>() {
            @Override
            public JQuickRow map(Tuple2<String, Double> tuple) throws Exception {
                JQuickRow result = parseGroupKey(tuple.f0, null);
                result.put(resultColumnName, tuple.f1);
                return result;
            }
        });
    }

    @Override
    protected Class<?> getResultType() {
        return Double.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickFlinkComputeTypeMedianImpl();
    }
    private static class JQuickFlinkComputeTypeMedianImpl extends JQuickFlinkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickAggregateProviderMethodConstants.MEDIAN;
        }
    }
}
